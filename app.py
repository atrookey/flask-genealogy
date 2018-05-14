from flask import Flask, request, render_template
from flask_mysqldb import MySQL
from ged4py import GedcomReader
from io import BytesIO


class IndividualProvider:

  def __init__(self, mysql):
    self.mysql = mysql

  def getFamilyRecords(self, individualRecord, familyTag):
    return self.getRecords(
        individualRecord,
        familyTag,
        '''SELECT f_gedcom FROM wt_families WHERE f_id = %s''',
        'FAM')

  def getIndividualRecords(self, familyRecord, individualTag):
    return self.getRecords(
        familyRecord,
        individualTag,
        '''SELECT i_gedcom FROM wt_individuals WHERE i_id = %s''',
        'INDI')

  def getRecords(self, sourceRecord, tag, sqlQuery, topLevelTag):
    records = []
    # Record.sub_tags appears to be broken.  Using this code instead.  Source model.py
    for subRecord in [x for x in (sourceRecord.sub_records or []) if x.tag == tag]:
      # Pointers used, gedcom incomplete.  Strip @ signs
      referenceId = subRecord.value.strip('@')
      cursor = self.mysql.connection.cursor()
      cursor.execute(sqlQuery, [referenceId])
      gedcom = cursor.fetchall()[0][0]
      with BytesIO(bytes(gedcom, 'utf_8')) as gedcom_file:
        parser = GedcomReader(gedcom_file)
        # For loop is misleading, only loops once.
        for record in parser.records0(topLevelTag):
          records.append(record)
    return records

  def firstElement(self, records):
    if records:
      return records[0]
    return None

  def recordIdString(self, record):
    return record.xref_id.strip('@')

  def getPartnerRecordForFamily(self, familySpouseRecord, individualRecord):
    wifeRecord = self.firstElement(
      self.getIndividualRecords(familySpouseRecord, 'WIFE'))
    # If individual is the wife, return the husband
    if wifeRecord and (self.recordIdString(wifeRecord) == self.recordIdString(individualRecord)):
      return self.firstElement(self.getIndividualRecords(familySpouseRecord, 'HUSB'))
    return wifeRecord

  def getIndividual(self, individualIdString):
    cursor = self.mysql.connection.cursor()
    cursor.execute('''SELECT i_gedcom FROM wt_individuals WHERE i_id = %s''', [
        individualIdString])
    rows = cursor.fetchall()
    if not rows:
      return None
    gedcom = rows[0][0] # Get the first row, gedcom column
    with BytesIO(bytes(gedcom, 'utf_8')) as gedcom_file:
      parser = GedcomReader(gedcom_file)
      for individualRecord in parser.records0('INDI'):
        individual = Individual(self.recordIdString(
            individualRecord), individualRecord.name.format())
        individual.sexString = individualRecord.sex
        if individualRecord.sub_tag('BIRT') and individualRecord.sub_tag('BIRT').sub_tag('DATE'):
          individual.bornString = individualRecord.sub_tag(
              'BIRT').sub_tag('DATE').value.fmt()
        if individualRecord.sub_tag('DEAT') and individualRecord.sub_tag('DEAT').sub_tag('DATE'):
          individual.bornString = individualRecord.sub_tag(
              'DEAT').sub_tag('DATE').value.fmt()
        # TODO(atrookey): May be buggy, need to support multiple child families
        familyChildRecord = self.firstElement(
            self.getFamilyRecords(individualRecord, 'FAMC'))
        if familyChildRecord:
          # TODO(atrookey): May be buggy, need to support multiple wives.
          motherRecord = self.firstElement(
              self.getIndividualRecords(familyChildRecord, 'WIFE'))
          if motherRecord:
            individual.addMother(motherRecord.name.format(),
                                 motherRecord.xref_id.strip('@'))
          # TODO(atrookey): May be buggy, need to support multiple husbands.
          fatherRecord = self.firstElement(
              self.getIndividualRecords(familyChildRecord, 'HUSB'))
          if fatherRecord:
            individual.addFather(fatherRecord.name.format(),
                                 fatherRecord.xref_id.strip('@'))
        familySpouseRecords = self.getFamilyRecords(
            individualRecord, 'FAMS')
        for familySpouseRecord in familySpouseRecords:
          family = Family(self.recordIdString(familySpouseRecord))
          partnerRecord = self.getPartnerRecordForFamily(
              familySpouseRecord, individualRecord)
          if partnerRecord:
            family.addPartner(partnerRecord.name.format(),
                              self.recordIdString(partnerRecord))
          childRecords = self.getIndividualRecords(
              familySpouseRecord, 'CHIL')
          for childRecord in childRecords:
            family.addChild(childRecord.name.format(),
                            childRecord.xref_id.strip('@'))
          individual.addFamily(family)
        return individual
    return None


class Individual:

  def __init__(self, individualIdString, nameString=None):
    self.individualIdString = individualIdString
    self.nameString = nameString
    self.sexString = 'Unknown'
    self.bornString = 'Unknown'
    self.diedString = 'Unknown'
    self.families = []
    self.motherIndividual = None
    self.fatherIndividual = None

  def addMother(self, nameString, individualIdString):
    self.motherIndividual = Individual(individualIdString, nameString)

  def addFather(self, nameString, individualIdString):
    self.fatherIndividual = Individual(individualIdString, nameString)

  def addFamily(self, family):
    self.families.append(family)


class Family:

  def __init__(self, familyId):
    self.familyId = familyId
    self.partnerIndividual = None
    self.childIndividuals = []

  def addChild(self, name, individualIdString):
    self.childIndividuals.append(Individual(individualIdString, name))

  def addPartner(self, nameString, individualIdString):
    self.partnerIndividual = Individual(individualIdString, nameString)


app = Flask(__name__)

app.config.from_envvar('APPLICATION_SETTINGS')

mysql = MySQL(app)

individualProvider = IndividualProvider(mysql)


@app.route('/', methods=['GET'])
def individual():
  try:
    assert 'i_id' in request.args
  except AssertionError:
    return str('Query fields \'i_id\' is required.')

  individualIdString = request.args['i_id']
  individual = individualProvider.getIndividual(individualIdString)
  if not individual:
    return render_template('not_found.html', individualIdString=individualIdString)
  return render_template('individual.html', individual=individual)


@app.route('/sources', methods=['GET'])
def sources():
  return render_template('not_found.html', individualIdString='sources')


if __name__ == '__main__':
  app.run(debug=True)
