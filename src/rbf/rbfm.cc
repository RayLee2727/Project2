#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    _pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
    _pf_manager = NULL;
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return _pf_manager->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    char *record = (char*)calloc(PAGE_SIZE, sizeof(char));
    char *page   = (char*)calloc(PAGE_SIZE, sizeof(char));
    short int slotNum, page_dir;
    char first = 1;
    unsigned short int freeSpace, recordCount, record_length, currPage;

    currPage = (fileHandle.getNumberOfPages() <= 0) ? 0 : fileHandle.getNumberOfPages()-1;
    for (;currPage < fileHandle.getNumberOfPages();){
        if (fileHandle.readPage(currPage, page) != 0) {
            free(page);
            return -1;
        }
        memcpy(&freeSpace,   &page[PAGE_SIZE - 2], sizeof(unsigned short int));
        memcpy(&recordCount, &page[PAGE_SIZE - 4], sizeof(unsigned short int));
        currPage = (first) ? currPage+1 : 0;
    }

    // pages don't have enough space
    if (currPage == fileHandle.getNumberOfPages()) {
        memset(page, 0, PAGE_SIZE);
        freeSpace = 0;
        recordCount = 0;
    }

    // append to dir
    recordCount++;
    rid.slotNum = recordCount;
    rid.pageNum = currPage;

    // overwrite slotNum and Count
    int i=0;
    while(i < recordCount-1){
        const short int delNum = 0x8000;
        memcpy(&slotNum, &page[PAGE_SIZE-4-(4*i)], sizeof(short int));
        if (slotNum == delNum) {
            rid.slotNum = i;
            recordCount=recordCount-1;
            break;
        }
        i++;
    }

    // write record in page dir
    page_dir = PAGE_SIZE - (4 * rid.slotNum + 4);
    record_length = packRecord(recordDescriptor, data, record);
    memcpy(page + freeSpace,    record,         record_length);
    memcpy(page + page_dir    , &freeSpace,     sizeof(unsigned short int));
    memcpy(page + page_dir+2,   &record_length, sizeof(unsigned short int));
    freeSpace += record_length;
    memcpy(page + PAGE_SIZE - 4, &recordCount, sizeof(unsigned short int));
    memcpy(page + PAGE_SIZE - 2, &freeSpace,   sizeof(unsigned short int));

    int append_rc = (currPage==fileHandle.getNumberOfPages()) ? fileHandle.appendPage(page) : fileHandle.writePage(currPage, page);
    if (append_rc != 0) {
        free(record);
        free(page);
        return -1;
    }

    free(record);
    free(page);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

    char *page = (char*) calloc(PAGE_SIZE, sizeof(char));
    short int record;
    unsigned short int fieldCount, pageNum, count;
    unsigned short int curr_offset, prev_offset;
    unsigned short int i=0;

    if(fileHandle.readPage(rid.pageNum, page) != 0) {
        free(page);
        return -1;
    }
    memcpy(&count, &page[PAGE_SIZE - 4], sizeof(unsigned short int));
    memcpy(&record, &page[PAGE_SIZE - 4 - (4 * rid.slotNum)], sizeof(short int));
    memcpy(&fieldCount, &page[record], sizeof(short int));

    if(rid.slotNum > count) {
        free(page);
        return -1;
    }
    if (fieldCount != recordDescriptor.size()) {
        free(page);
        return -1;
    }

//    cout << "field count" << fieldCount << endl;
//    cout << "record size" << recordDescriptor.size() << endl;
//    cout << "record offset" << record << endl;
//    cout << "slot num"   << rid.slotNum << endl;
    if (record < 0) {
        memcpy(&pageNum,  &page[PAGE_SIZE - 2 - (4 * rid.slotNum)], sizeof(unsigned short int));

        RID new_rid;
        new_rid.pageNum = pageNum;
        new_rid.slotNum = (-1)*record;

        free(page);

        return readRecord(fileHandle, recordDescriptor, new_rid, data);
    }

    memcpy(data, &page[record + sizeof(unsigned short int)], (unsigned short int)ceil(fieldCount / 8.0));
    curr_offset = prev_offset = sizeof(unsigned short int) + (unsigned short int)ceil(fieldCount / 8.0) + fieldCount * sizeof(unsigned short int);
    char *data_c = (char*)data + (unsigned short int)ceil(fieldCount / 8.0);

    // convert page format to [vector + data] format
    while (i < fieldCount){
        if (!(*((char*)data + (char)(i/8)) & (1<<(7-i%8)))) {
            memcpy(&curr_offset, &page[record + sizeof(unsigned short int) + (unsigned short int)ceil(fieldCount / 8.0) + i * sizeof(uint16_t)], sizeof(uint16_t));
            if (recordDescriptor[i].type != TypeVarChar){
                memcpy(&data_c[0], &page[record + prev_offset], sizeof(int));
                data_c += sizeof(int);
            }
            else{
                int attlen = curr_offset - prev_offset;
                memcpy(&data_c[0], &attlen, sizeof(int));
                memcpy(&data_c[4], &page[record + prev_offset], attlen);
                data_c += (4 + attlen);
            }
        }
        prev_offset = curr_offset;
        i++;
    }

    free(page);
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
    char *page = (char*) calloc(PAGE_SIZE, sizeof(char));

    unsigned short int record_count, free_space, pageNum, record_length;
    short int record_offset, i_record_offset;
    const short int delOffset = 0x8000;
    RID new_rid;

    fileHandle.readPage(rid.pageNum, page);
    if(fileHandle.readPage(rid.pageNum, page) != 0) {
        free(page);
        return -1;
    }
    memcpy(&record_offset, &page[PAGE_SIZE - 4 - (4 * rid.slotNum)], sizeof(short int));
    memcpy(&record_count,  &page[PAGE_SIZE - 4],                     sizeof(unsigned short int));
    memcpy(&free_space,    &page[PAGE_SIZE - 2],                     sizeof(unsigned short int));

    if(rid.slotNum > record_count) {
        free(page);
        return -1;
    }

    if (record_offset < 0) {

        memcpy(&pageNum,  &page[PAGE_SIZE - 2 - (4 * rid.slotNum)], sizeof(unsigned short int));
        new_rid.pageNum = pageNum;
        new_rid.slotNum = record_offset * (-1);

        // delete record from page then overwrite page directory
        deleteRecord(fileHandle, recordDescriptor, new_rid);
        const short int delNum=0x8000;
        memcpy(&page[PAGE_SIZE - 4 - (4 * rid.slotNum)], &delNum, sizeof(short int));
        fileHandle.writePage(rid.pageNum, page);

        free(page);
        return 0;
    }
    // remove target record then update page dir
    memcpy(&record_length,        &page[PAGE_SIZE - 2 - (4 * rid.slotNum)], sizeof(unsigned short int));
    memmove(&page[record_offset], &page[record_offset + record_length],     free_space - record_length - record_offset);
    free_space -= record_length;
    memcpy(&page[PAGE_SIZE - 2],  &free_space,                sizeof(unsigned short int));

    for (int i = 1; i <= record_count; i++) {
        memcpy(&i_record_offset, &page[PAGE_SIZE - 4 - (4 * i)], sizeof(short int));
        if (i_record_offset > record_offset) {
            memcpy(&page[PAGE_SIZE - 4 - (4 * i)], &i_record_offset - record_length, sizeof(unsigned short int));
        }
    }

    memcpy(&page[PAGE_SIZE - 4 - (4 * rid.slotNum)], &delOffset, sizeof(short int));
    fileHandle.writePage(rid.pageNum, page);
    free(page);
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){

    char *page = (char*) calloc(PAGE_SIZE, sizeof(char));
    short int slotNum;
    unsigned short int pageNum;
    RID new_rid;

    deleteRecord(fileHandle, recordDescriptor, rid);
    if (checkRecord(fileHandle, recordDescriptor, data, rid)==true) return 0;
    else{
        insertRecord(fileHandle, recordDescriptor, data, new_rid);
        fileHandle.readPage(rid.pageNum, page);

        // overwrite page directory
        pageNum = new_rid.pageNum;
        slotNum = new_rid.slotNum * (-1);

        memcpy(&page[PAGE_SIZE - 4 - (4 * rid.slotNum)], &slotNum, sizeof(short int));
        memcpy(&page[PAGE_SIZE - 2 - (4 * rid.slotNum)], &pageNum, sizeof(unsigned short int));

        fileHandle.writePage(rid.pageNum, page);
    }
    free(page);
    return 0;

}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
    char *page = (char*) calloc(PAGE_SIZE, sizeof(char));
    unsigned short int count, pageNum, fieldCount, head, tail, i=0;
    short int record;
    RID newRid;

    if(fileHandle.readPage(rid.pageNum, page) != 0) {
        free(page);
        return -1;
    }

    memcpy(&count,      &page[PAGE_SIZE - 4],                     sizeof(unsigned short int));
    memcpy(&record,     &page[PAGE_SIZE - (4 * rid.slotNum)-4], sizeof(short int));
    memcpy(&fieldCount, &page[record],                            sizeof(unsigned short int));

    if(rid.slotNum > count) {
        free(page);
        return -1;
    }

    if (record < 0) {
        memcpy(&pageNum,  &page[PAGE_SIZE - (4 * rid.slotNum)-2], sizeof(unsigned short int));
        newRid.pageNum = pageNum;
        newRid.slotNum = (-1)* record;
        free(page);
        return readAttribute(fileHandle, recordDescriptor, newRid, attributeName, data);
    }

    if (fieldCount != recordDescriptor.size()) {
        free(page);
        return -1;
    }

    while(i < recordDescriptor.size()){
        if (recordDescriptor[i].name == attributeName) break;
        i++;
    }

    if (!(page[record + sizeof(uint16_t) + i/8] & (1<<(7-i%8)))) memset(data, 0, 1);
    else                                                         memset(data, 0x8000, 1);

    if (i!=0) memcpy(&head, &page[record + sizeof(unsigned short int) + (unsigned short int)ceil(fieldCount / 8.0) + (i-1)*sizeof(unsigned short int)], sizeof(unsigned short int));
    else           head = sizeof(unsigned short int) + (unsigned short int)ceil(fieldCount / 8.0) + fieldCount * sizeof(unsigned short int);

    memcpy(&tail, &page[record + sizeof(unsigned short int) + (unsigned short int)ceil(fieldCount / 8.0) + i*sizeof(unsigned short int)], sizeof(unsigned short int));
    if (recordDescriptor[i].type != TypeVarChar)  memcpy((char*)data + 1, &page[record + head], sizeof(int));
    else {
        memcpy((char*)data + 1, &tail - head, sizeof(int));
        memcpy((char*)data + 1 + sizeof(int), &page[record + head], tail - head);
    }
    free(page);
    return 0;
}


RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {

    unsigned short int offset = (unsigned short int)ceil(recordDescriptor.size()/8.0);
    const char* dataArray = (char*)data;
    unsigned int int_num, char_len, i=0;
    float real_num;

    cout << "print record: record size: " << recordDescriptor.size() << endl;

    for( ;i < (unsigned)recordDescriptor.size();i++){
    	cout << "init value of i=" << i << endl;

        if (!(*(dataArray + (char)(i/8)) & (1<<(7-i%8)))) {
            if (recordDescriptor[i].type == TypeInt) {
                memcpy(&int_num, &dataArray[offset], sizeof(int));
                cout << recordDescriptor[i].name << ": " << int_num << endl;
                offset += sizeof(int);
                cout << "is int" << endl;
            }
            else if (recordDescriptor[i].type == TypeReal) {
                memcpy(&real_num, &dataArray[offset], sizeof(float));
                cout << recordDescriptor[i].name << ": " << real_num << endl;
                offset += sizeof(float);
            }
            else if (recordDescriptor[i].type == TypeVarChar) {
                memcpy(&char_len, &dataArray[offset], sizeof(int));
                char content[char_len + 1];
                memcpy(content, &dataArray[offset + sizeof(int)], char_len );
                content[char_len] = 0;
                cout << recordDescriptor[i].name << ": " << content << endl;
                offset += (4 + char_len);
            }

        }
        else cout << recordDescriptor[i].name << ": NULL" << endl;
       // i++;
        cout << "value of i=" << i << endl;
    }
    cout << "print record: finish" << endl;
    return 0;
}



bool RecordBasedFileManager::checkRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {

    char *record = (char*)calloc(PAGE_SIZE, sizeof(char));
    char *page = (char*)calloc(PAGE_SIZE, sizeof(char));

    unsigned short int record_length, recordCount, freeSpace;
    record_length = packRecord(recordDescriptor, data, record);

    // read the page and get page info
    fileHandle.readPage(rid.pageNum, page);
    memcpy(&freeSpace,   &page[PAGE_SIZE - 2], sizeof(unsigned short int));
    memcpy(&recordCount, &page[PAGE_SIZE - 4], sizeof(unsigned short int));

    // write in record and update page directory
    memcpy(page + freeSpace, record, record_length);
    memcpy(page + PAGE_SIZE - (4 * rid.slotNum + 4)    , &freeSpace,     sizeof(signed short int));
    memcpy(page + PAGE_SIZE - (4 * rid.slotNum + 4) + 2, &record_length, sizeof(signed short int));
    freeSpace = freeSpace+ record_length;
    memcpy(page + PAGE_SIZE - 2, &freeSpace, sizeof(unsigned short int));
    fileHandle.writePage(rid.pageNum, page);

    free(record);
    free(page);

    return true;

}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
                                const vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute,
                                const CompOp compOp,                  // comparision type such as "<" and "="
                                const void *value,                    // used in the comparison
                                const vector<string> &attributeNames, // a list of projected attributes
                                RBFM_ScanIterator &rbfm_ScanIterator) {

    rbfm_ScanIterator.fh = &fileHandle;
    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.value = (void*) value;
    rbfm_ScanIterator.attributeNamesSize = attributeNames.size();

    rbfm_ScanIterator.page = (char*)malloc(PAGE_SIZE);
    rbfm_ScanIterator.currSlot = 0;
    rbfm_ScanIterator.currPageNum = -1;

    set<unsigned short int> tempSet;
        for (int i = 0; i < (unsigned short int)recordDescriptor.size(); ++i) {
            for (int j = 0; j < attributeNames.size(); ++j) {
                if (attributeNames[j] == recordDescriptor[i].name) {
                	tempSet.insert(i);
                    break;
                }
            }
        }

        if (conditionAttribute == "") rbfm_ScanIterator.conditionAttributeIndex = rbfm_ScanIterator.ALL;

			for (int i = 0; i < (unsigned short int)recordDescriptor.size(); ++i) {
				if (conditionAttribute == recordDescriptor[i].name) rbfm_ScanIterator.conditionAttributeIndex = i;
			}

        rbfm_ScanIterator.scanSet = tempSet;


    return 0;
}

//*************//

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
bool found=false;
bool notWritten = true;
short int record;
unsigned short int fieldCount, directory, offset, prev_offset, null_indicator_count=0;

//cout << "before loop found = " << found << endl;
while(!found){
//	cout << "curr page num " << currPageNum << endl;

    if (lastSlot == currSlot || currPageNum == -1 ) {

//    	cout << "if: curr page num " << currPageNum << endl;
//        	cout << "if: curr slot " << currSlot << endl;
//        	cout << "if: last slot " << lastSlot << endl;
    	currPageNum++;
    	int rc = fh->readPage(currPageNum, page);
        if (rc != 0) return -1;
        memcpy(&lastSlot, &page[PAGE_SIZE - 4], sizeof(unsigned short int));
//        cout << "last slot" << lastSlot << endl;
        currSlot = 0;
    }

    currSlot++;
    memcpy(&record, &page[PAGE_SIZE - 4 - 4 * currSlot], sizeof(short int));
//        cout << "curr slot: " << currSlot << endl;
//        cout << "record offset: " << record << endl;
    while(record == -32768) {
    	int rc = fh->readPage(++currPageNum, page);
    	if (rc != 0) return -1;
    	memcpy(&record, &page[PAGE_SIZE - 4 - 4 * currSlot], sizeof(short int));
    }
    memcpy(&fieldCount, &page[record], sizeof(short int));
//    cout << "field count: " << fieldCount << endl;
//    cout << "record size: " << recordDescriptor.size() << endl;
    if (fieldCount != recordDescriptor.size()) return -1;

    unsigned short int nullSize = (unsigned short int)ceil(attributeNamesSize/8.0);
    char *data_c = (char*)data + nullSize;
    char* nullIndicator = (char*)malloc(nullSize);
    memset(nullIndicator, 0, nullSize);

    directory = (unsigned short int)ceil(fieldCount / 8.0) + record + sizeof(unsigned short int);
    offset = (unsigned short int)ceil(fieldCount/8.0) + fieldCount*sizeof(unsigned short int)+sizeof(unsigned short int);
    prev_offset = offset;

    int i=0;
    while(i<fieldCount){
    	memcpy(&offset, &page[directory + i*sizeof(unsigned short int)], sizeof(unsigned short int));
//    	cout << "conditional scan index: " << conditionAttributeIndex << endl;
    	if (compOp == NO_OP) found=true;
    	else {
    	if (i == conditionAttributeIndex) {


    	            if (page[record + sizeof(unsigned short int) + i/8] & (1<<(7-i%8))) {
//    	            	if (compOp == EQ_OP) found = true;
    	            	found = false;
//    	            	else found=false;
    	            }
    	            else if (recordDescriptor[i].type == TypeVarChar) {
    	                int attlen = offset - prev_offset;
    	                string val = string(&page[record + prev_offset], attlen);
    	                if (vc_comp(val)==0) found=false;else found=true;
    	            } else if (recordDescriptor[i].type == TypeInt) {
    	                int val;
    	                memcpy(&val, &page[record + prev_offset], sizeof(int));
    	                if (int_comp(val)==0) found=true;else found=false;
    	            } else if (recordDescriptor[i].type == TypeReal) {
    	                float val;
    	                memcpy(&val, &page[record + prev_offset], sizeof(float));
    	                if (float_comp(val)==0) found=false;else found=true;
    	            }
    	        }
    	    }

    	if (found && notWritten) {
//    		bool writePermit = true;
    		int j = 0;
    		unsigned short int off = (unsigned short int)ceil(fieldCount/8.0) + fieldCount*sizeof(unsigned short int)+sizeof(unsigned short int);
    		unsigned short int prev_off = off;
    		while(j < fieldCount) {
    			memcpy(&off, &page[directory + j*sizeof(unsigned short int)], sizeof(unsigned short int));
    			if (scanSet.count(j)) {
					if (page[record + sizeof(unsigned short int) + j/8] & (1<<(7-j%8))) {
//						if (j == conditionAttributeIndex && compOp != EQ_OP) found = false;
						nullIndicator[null_indicator_count/8] |=  (1 << (7 - null_indicator_count%8));
					}
					else {
						int attlen = off - prev_off;
						if (recordDescriptor[j].type != TypeVarChar) {
							memcpy(&data_c[0], &page[record + prev_off], sizeof(int));
							data_c += sizeof(int);
						}
						else {
							memcpy(&data_c[0], &attlen, sizeof(int));
							memcpy(&data_c[4], &page[record + prev_off], attlen);
							data_c += (4 + attlen);
						}
						nullIndicator[null_indicator_count/8] &= ~(1 << (7 - null_indicator_count%8));
					}
					null_indicator_count++;
				}
    			prev_off = off;
    			j++;
    		}
    		notWritten = false;
    		memcpy(data, nullIndicator, nullSize);
    	}
				prev_offset = offset;
				i++;


    }

    free(nullIndicator);
//    cout << "------" << endl;
//    cout << "found:  " << found << endl;
//    cout << "------" << endl;
}

    rid.slotNum = currSlot;
    rid.pageNum = currPageNum;
    return 0;
}

RC RBFM_ScanIterator::close() {
    free(page);
    return 0;
}

unsigned short int RecordBasedFileManager::packRecord(const vector<Attribute> &recordDescriptor, const void *data, char *record) {

    //creates a record in page formated from [vector+data], return record size at the end

    const char* dataArray = (char*)data;
    int attlen, i=0;
    unsigned short int count, in_offset, dir_offset, recordOffset;

    count = (unsigned short int)recordDescriptor.size();
    memcpy(record, &count, sizeof(unsigned short int));

    in_offset = (unsigned short int)ceil(count/8.0);
    memcpy(record + sizeof(uint16_t), &dataArray[0], in_offset);

    dir_offset = sizeof(unsigned short int) + in_offset;
    recordOffset = sizeof(unsigned short int) * count + dir_offset;

    while(i<count){
        if (!(*(dataArray + (char)(i/8)) & (1<<(7-i%8)))) {

            if (recordDescriptor[i].type == TypeInt) {
                char* new_record = record+recordOffset;
                memcpy(new_record, &dataArray[in_offset], sizeof(int));
                in_offset += sizeof(int);
                recordOffset += sizeof(int);
                new_record=record+dir_offset;
                memcpy(new_record, &recordOffset, sizeof(unsigned short int));
            }
            else if (recordDescriptor[i].type == TypeReal) {
                char* new_record;
                new_record=record + recordOffset;
                memcpy(record + recordOffset, &dataArray[in_offset], sizeof(float));
                in_offset += sizeof(float);
                recordOffset += sizeof(float);
                new_record=record+dir_offset;
                memcpy(new_record, &recordOffset, sizeof(unsigned short int));
            }
            else if (recordDescriptor[i].type == TypeVarChar) {
                char* new_record;
                memcpy(&attlen,              &dataArray[in_offset],     sizeof(int));
                new_record=record + recordOffset;
                memcpy(new_record, &dataArray[in_offset + 4], attlen);
                recordOffset += attlen;
                in_offset += (4 + attlen);
                new_record=record + dir_offset;
                memcpy(new_record, &recordOffset, sizeof(unsigned short int));
            }
        }
        else {
            char* new_record = record+dir_offset;
            memcpy(new_record, &recordOffset, sizeof(unsigned short int));
        }
        dir_offset += sizeof(unsigned short int);
        i++;
    }
    return recordOffset;
}

unsigned int RBFM_ScanIterator::float_comp(float val) {
    float val2 = *(float*)value;
    switch (compOp) {
            case EQ_OP: return !(val2 == val);
            case LT_OP: return (val2 <= val);
            case GT_OP: return (val2 >= val);
            case LE_OP: return (val2 <  val);
            case GE_OP: return (val2 >  val);
            case NE_OP: return !(val2 != val);
        default: return 0;
    }
}

unsigned int RBFM_ScanIterator::int_comp(int val) {
    int val2 = *(int*)value;
    switch (compOp) {
            case EQ_OP:
//            cout << "&&&&&&&" << !(val2 == val) << endl;
            return !(val2 == val);
            case LT_OP:
//            cout << "((((((((" << (val2 <= val) << endl;
            return (val2 <= val);
            case GT_OP: return (val2 >= val);
            case LE_OP: return (val2 <  val);
            case GE_OP: return (val2 >  val);
            case NE_OP: return !(val2 != val);
        default: return 0;
    }
}

unsigned int RBFM_ScanIterator::vc_comp(string val) {
    string val2 = string((char*)(value));
    int cmp = strcmp(val2.c_str(), val.c_str());
    switch (compOp) {
            case EQ_OP: return cmp == 0;
            case LT_OP: return !(cmp <= 0);
            case GT_OP: return !(cmp >= 0);
            case LE_OP: return !(cmp <  0);
            case GE_OP: return !(cmp >  0);
            case NE_OP: return cmp != 0;
        default: return 0;
    }
}

//RC RBFM_ScanIterator::initRids(FileHandle &fileHandle, vector<RID> &rids) {
//	RID rid;
//	char* pg = (char*)malloc(PAGE_SIZE + 1);
//	pg[PAGE_SIZE] = '\0';
//	for (int i = 0; i < fileHandle.getNumberOfPages(); i++) {
//		if (fh->readPage(i, pg) != 0) {
//			free(pg);
//			return -1;
//		}
//		int count;
//		memcpy(&count, &pg[PAGE_SIZE - 4], sizeof(unsigned short int));
//		for(int j = 1; j <= count; j++) {
//			rid.pageNum = i;
//			rid.slotNum = j;
//			rids.push_back(rid);
//		}
//	}
//	free(pg);
//	return 0;
//}
