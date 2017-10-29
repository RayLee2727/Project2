#include "rm.h"
#include <iostream>


RelationManager* RelationManager::instance()
{
	static RelationManager _rm;
	return &_rm;
}

RelationManager::RelationManager()
{
   _rbfm = RecordBasedFileManager::instance();
   FILE* exists = fopen("tables.stat", "r");
   if(exists == NULL) {
        stats = fopen("tables.stat", "wrb+");
   } else {
       fclose(exists);
       stats = fopen("tables.stat", "rb+");
   }
}

RelationManager::~RelationManager()
{
   _rbfm = NULL;
   fflush(stats);
   fclose(stats);
}
vector<Attribute> RelationManager::vec_table() {
   vector<Attribute> record;
   getTableAttr(record);

   return record;
}

vector<Attribute> RelationManager::vec_column() {
   vector<Attribute> record;
   getColumnAttr(record);

   return record;
}



RC RelationManager::createCatalog()
{
  int table_rc, col_rc, maxID;
  FileHandle fh1; FileHandle fh2;

  _rbfm->createFile("Tables.txt");
  _rbfm->createFile("Columns.txt");

    // insert table records
    _rbfm->openFile("Tables.txt", fh1);
    //get table id
    fseek(stats, 0, SEEK_SET);
    fread(&maxID, sizeof(int), 1, stats);
    maxID = maxID + 1;
    //insert record
    insertTableRecord(fh1, maxID,   "Tables",  "Tables.txt");
    insertTableRecord(fh1, maxID+1, "Columns", "Columns.txt");

    _rbfm->closeFile(fh1);
    // set table id
    fseek(stats, 0, SEEK_SET);
    fwrite(&maxID, sizeof(int), 1, stats);
    fflush(stats);
    table_rc = 0;

    // insert column records
    _rbfm->openFile("Columns.txt", fh2);

    insertColumnRecord(fh2, 1, "table-id",   TypeInt,      4, 1);
    insertColumnRecord(fh2, 1, "table-name", TypeVarChar, 50, 2);
    insertColumnRecord(fh2, 1, "file-name",  TypeVarChar, 50, 3);
//    insertColumnRecord(fh2, 1, "privileged", TypeInt,      4, 4);

    insertColumnRecord(fh2, 2, "table-id",        TypeInt,      4, 1);
    insertColumnRecord(fh2, 2, "column-name",     TypeVarChar, 50, 2);
    insertColumnRecord(fh2, 2, "column-type",     TypeInt,      4, 3);
    insertColumnRecord(fh2, 2, "column-length",   TypeInt,      4, 3);
    insertColumnRecord(fh2, 2, "column-position", TypeInt,      4, 4);

    _rbfm->closeFile(fh2);
    col_rc = 0;

    if(table_rc != 0 && col_rc != 0) return -1;
    return 0;
}

RC RelationManager::deleteCatalog()
{
    _rbfm->destroyFile("Tables.txt");
    _rbfm->destroyFile("Columns.txt");
    remove("tables.stat");

    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
  FileHandle fh;
  int maxId, i=0;

   _rbfm->createFile(tableName + ".txt");
   _rbfm->openFile("Tables.txt", fh);
   // get table id
   fseek(stats, 0, SEEK_SET);
   fread(&maxId, sizeof(int), 1, stats);
   maxId = maxId+1;

   // insert table record, system attribute = 1
   insertTableRecord(fh, maxId, tableName, tableName + ".txt");
   // set table id
   fseek(stats, 0, SEEK_SET);
   fwrite(&maxId, sizeof(int), 1, stats);
   fflush(stats);
   _rbfm->closeFile(fh);
   _rbfm->openFile("Columns.txt", fh);

   for(; i < attrs.size(); i++) {
      Attribute attr = attrs.at(i);
      insertColumnRecord(fh, maxId, attr.name, attr.type, attr.length, i+1);
   }

   _rbfm->closeFile(fh);
   return 0;
}

//helper function
RC RelationManager::insertTableRecord(FileHandle &fh, const int tableId, const string name, const string fileName) {

   RID rid;
   int recordSize = 0, offset = 0, nullSize = 1;
   void *record =      malloc(1 + sizeof(int) + sizeof(int) + name.length() + sizeof(int) + fileName.length() + sizeof(int));
   void *finalRecord = malloc(1 + sizeof(int) + sizeof(int) + name.length() + sizeof(int) + fileName.length() + sizeof(int));
   vector<Attribute> recordDescriptor = vec_table();

   unsigned char *null = (unsigned char*) malloc(nullSize);
   memset(null, 0, nullSize);
   memcpy((char *)record + offset, null, nullSize);
   offset += nullSize;
   memcpy((char *)record + offset, &tableId, sizeof(int));
   offset += sizeof(int);
   auto nameLen = name.length();
   memcpy((char *)record + offset, &nameLen, sizeof(int));
   offset += sizeof(int);
   memcpy((char *)record + offset, name.c_str(), name.length());
   offset += name.length();
   auto fileLen = fileName.length();
   memcpy((char *)record + offset, &fileLen, sizeof(int));
   offset += sizeof(int);
   memcpy((char *)record + offset, fileName.c_str(), fileName.length());
   offset += fileName.length();
   recordSize = offset;

   _rbfm->insertRecord(fh, recordDescriptor, record, rid);
   _rbfm->readRecord(fh, recordDescriptor, rid, finalRecord);

   free(null);
   free(finalRecord);
   free(record);
   return 0;
}

//helper function
RC RelationManager::insertColumnRecord(FileHandle &fh, const int tableId, const string name, const int colType, const int colLength, const int colPos) {

   RID rid;
   int recordSize = 0;int offset = 0; int nullSize = 1;
   unsigned char *nullsIndicator = (unsigned char *) malloc(nullSize);
   void *record =      malloc(1 + sizeof(int) + sizeof(int) + name.length() + sizeof(int) + sizeof(int) + sizeof(int));
   void *finalRecord = malloc(1 + sizeof(int) + sizeof(int) + name.length() + sizeof(int) + sizeof(int) + sizeof(int));

   vector<Attribute> recordDescriptor = vec_column();

   memset(nullsIndicator, 0, nullSize);
   memcpy((char *)record + offset, nullsIndicator, nullSize);
   offset += nullSize;
   memcpy((char *)record + offset, &tableId, sizeof(int));
   offset += sizeof(int);
   auto nameLen = name.length();
   memcpy((char *)record + offset, &nameLen, sizeof(int));
   offset += sizeof(int);
   memcpy((char *)record + offset, name.c_str(), nameLen);
   offset += nameLen;
   memcpy((char *)record + offset, &colType, sizeof(int));
   offset += sizeof(int);
   memcpy((char *)record + offset, &colLength, sizeof(int));
   offset += sizeof(int);
   memcpy((char *)record + offset, &colPos, sizeof(int));
   offset += sizeof(int);
   recordSize = offset;

   _rbfm->insertRecord(fh, recordDescriptor, record, rid);
   _rbfm->readRecord(fh, recordDescriptor, rid, finalRecord);

   free(nullsIndicator);
   free(finalRecord);
   free(record);

   return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    FileHandle fh1;FileHandle fh2;
    _rbfm->openFile("Tables.txt", fh1);

    RBFM_ScanIterator rbsi; RID rid;
//    RM_ScanIterator rmsi;

    vector<string> tableAttrs;
    tableAttrs.push_back("table-id");

    int rc = _rbfm->scan(fh1, vec_table(), "table-name", EQ_OP, tableName.c_str(), tableAttrs, rbsi);
    if (rc!=0) {cout << "error: scan" << endl; return -1;}

    rbsi.close();
    _rbfm->deleteRecord(fh1, vec_table(), rid);
    _rbfm->closeFile(fh1);

    int tableId;
    void *finalData = malloc(PAGE_SIZE);
    memcpy(&tableId, (char *)finalData + 1, sizeof(int));

    _rbfm->openFile("Columns.txt", fh2);

    const char* args2[] = {"column-name", "column-type", "column-length"};
    vector<string> colAttrs(args2, end(args2));
    _rbfm->scan(fh2, vec_column(), "table-id", EQ_OP, (void *)&tableId, colAttrs, rbsi);

    vector<RID> rids;

    while(rbsi.getNextRecord(rid, finalData) != RM_EOF) rids.push_back(rid);

    rbsi.close();
    for (auto value : rids) _rbfm->deleteRecord(fh2, vec_column(), value);

    _rbfm->closeFile(fh2);
    _rbfm->destroyFile(tableName + ".txt");

    free(finalData);
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    if (tableName == "Tables.txt") {
    	getTableAttr(attrs);
    	return 0;
    }
    if (tableName == "Columns.txt") {
    	getColumnAttr(attrs);
    	return 0;
    }

    RM_ScanIterator columnRsi;
    RID rid;
    void *data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE);
    vector<string> returnAttr;
    Attribute attr;
    int tid;
    returnAttr.push_back("column-name");
    returnAttr.push_back("column-type");
    returnAttr.push_back("column-length");

    string tn;
    tn = tableName + ".txt";
    if(getTidByTname(tn, tid) != 0) return -1;
    if(scan("Columns.txt", "table-id", EQ_OP, &tid, returnAttr, columnRsi) != 0) return -1;
    while(columnRsi.getNextTuple(rid, data) != RM_EOF) {
    	int len;
    	int offset = 0;

    	memcpy(&len, (char*)data, sizeof(int));
    	offset += sizeof(int);
    	char* charAttrName = (char*)malloc(len + 1);
    	charAttrName[len] = '\0';
    	memcpy(charAttrName, (char*)data + offset, len);
    	attr.name = charAttrName;
    	offset += len;

    	memcpy(&attr.type, (char*)data + offset, sizeof(int));
    	offset += sizeof(int);

    	memcpy(&attr.length, (char*)data + offset, sizeof(int));

    	attrs.push_back(attr);
    }
    columnRsi.close();
	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0) return -1;

	FileHandle fh;
	if(_rbfm->openFile(tableName, fh) != 0) return -1;

	if(_rbfm->insertRecord(fh, attrs, data, rid) != 0) return -1;
	if(_rbfm->closeFile(fh) != 0) return -1;

    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0) return -1;

	FileHandle fh;
	if(_rbfm->openFile(tableName, fh) != 0) return -1;

	if(_rbfm->deleteRecord(fh, attrs, rid) != 0) return -1;
	if(_rbfm->closeFile(fh) != 0) return -1;
    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0) return -1;

	FileHandle fh;
	if(_rbfm->openFile(tableName, fh) != 0) return -1;

	if(_rbfm->updateRecord(fh, attrs, data, rid) != 0) return -1;
	if(_rbfm->closeFile(fh) != 0) return -1;
	return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0) return -1;

	FileHandle fh;
	if(_rbfm->openFile(tableName, fh) != 0) return -1;

	if(_rbfm->readRecord(fh, attrs, rid, data) != 0) return -1;
	if(_rbfm->closeFile(fh) != 0) return -1;
	return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{

    return (_rbfm->printRecord(attrs, data));
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0) return -1;

	FileHandle fh;
	if(_rbfm->openFile(tableName, fh) != 0) return -1;

	if(_rbfm->readAttribute(fh, attrs, rid, attributeName, data) != 0) return -1;
	if(_rbfm->closeFile(fh) != 0) return -1;
    return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,
      const void *value,
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	vector<Attribute> attrs;
	FileHandle fh;
	RBFM_ScanIterator rbsi;

	if(_rbfm->openFile(tableName, fh) != 0) return -1;

	if(tableName == "Tables.txt") getTableAttr(attrs);
	else if(tableName == "Columns.txt") getColumnAttr(attrs);
	else {
		if (getAttributes(tableName, attrs) != 0) return -1;
	}


	if(_rbfm->scan(fh, attrs, conditionAttribute, compOp, value, attributeNames, rbsi) != 0) return -1;
	rm_ScanIterator.set_rbsi(&rbsi);
	rbsi.close();
	if(_rbfm->closeFile(fh) != 0) return -1;
	return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

void RelationManager::getTableAttr(vector<Attribute> &attrs) {
	Attribute attribute;

	attribute.name = "table-id";
	attribute.type = TypeInt;
	attribute.length = (AttrLength)4;
	attrs.push_back(attribute);

	attribute.name = "table-name";
	attribute.type = TypeVarChar;
	attribute.length = (AttrLength)50;
	attrs.push_back(attribute);

	attribute.name = "file-name";
	attribute.type = TypeVarChar;
	attribute.length = (AttrLength)50;
	attrs.push_back(attribute);
}

void RelationManager::getColumnAttr(vector<Attribute> &attrs) {
	Attribute attribute;

	attribute.name = "table-id";
	attribute.type = TypeInt;
	attribute.length = (AttrLength)4;
	attrs.push_back(attribute);

	attribute.name = "column-name";
	attribute.type = TypeVarChar;
	attribute.length = (AttrLength)50;
	attrs.push_back(attribute);

	attribute.name = "column-type";
	attribute.type = TypeInt;
	attribute.length = (AttrLength)4;
	attrs.push_back(attribute);

	attribute.name = "column-length";
	attribute.type = TypeInt;
	attribute.length = (AttrLength)4;
	attrs.push_back(attribute);

	attribute.name = "column-position";
	attribute.type = TypeInt;
	attribute.length = (AttrLength)4;
	attrs.push_back(attribute);
}

RC RelationManager::getTidByTname(const string &tableName, int & tid) {
	RM_ScanIterator rsi;
	RID rid;
	void *data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);
	vector<string> tidAttr;
	Attribute attr;
	tidAttr.push_back("table-id");

	if(scan("Tables.txt", "table-name", EQ_OP, &tableName, tidAttr, rsi) != 0) return -1;
	if(rsi.getNextTuple(rid, data) != RM_EOF) {
		memcpy(&tid, data, sizeof(int));
	}
	rsi.close();

	return 0;
}
