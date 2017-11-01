#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    FILE * file;
	// check if file exists
    struct stat buf;
	if(stat(fileName.c_str(), &buf) == 0) return -1;
	//create file
	file = fopen(fileName.c_str(), "wb");
	fclose(file);
	return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	if (remove(fileName.c_str()) != 0) return -1;
	else return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{

    if(fileHandle.openedFile != NULL) return -1;
    // cout <<"fileHandle was not null" << endl;

    struct stat fileInfo;
    if(stat(fileName.c_str(), &fileInfo) != 0) return -1;
    // cout <<"fileinfo extists" << endl;

    fileHandle.openedFile = fopen(fileName.c_str(), "rb+");
    if(fileHandle.openedFile == NULL) return -1;
    // cout <<"file opened successfully" << endl;

    fileHandle.fileSize = fileInfo.st_size;

//    FILE * file;
//	file = fopen(fileName.c_str(),"rb+");
//	//check existence
//	if(!file) return -1;
//	//check if fileHandle used
//	if (fileHandle.getFile() != NULL) return -1;
//	fileHandle.setFile(file);

	// for further implementation
//		if(fileHandle.getNumberOfPages() == 0) {
//		void * data = malloc(sizeof(unsigned) * 3);
//		int offset = 0;
//		int * temp;
//		*temp = fileHandle.readPageCounter;
//		memcpy(data + offset, temp, sizeof(unsigned));
//		*temp = fileHandle.writePageCounter;
//		offset += sizeof(unsigned);
//		memcpy(data + offset, temp, sizeof(unsigned));
//		offset += sizeof(unsigned);
//		*temp = fileHandle.appendPageCounter;
//		memcpy(data + offset, temp, sizeof(unsigned));
//		fileHandle.appendPage(data);
//		fileHandle.appendPageCounter--;
//	} else {
//		void * data = malloc(sizeof(unsigned) * 3);
//		fread(data, sizeof(unsigned), 3, file);
//		int offset = 0;
//		int * temp;
//		*temp = fileHandle.readPageCounter;
//		memcpy(temp, data + offset, sizeof(unsigned));
//		*temp = fileHandle.writePageCounter;
//		offset += sizeof(unsigned);
//		memcpy(temp, data + offset, sizeof(unsigned));
//		offset += sizeof(unsigned);
//		*temp = fileHandle.appendPageCounter;
//		memcpy(temp, data + offset, sizeof(unsigned));

    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if(fileHandle.openedFile == NULL) return -1;

    fclose(fileHandle.openedFile);
    fileHandle.openedFile = NULL;

    return 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    fileSize = 0;
    openedFile = NULL;
}


FileHandle::~FileHandle()
{
//    if(openedFile == NULL) return;
//
//    fflush(openedFile);
//    fclose(openedFile);
//
//    openedFile = NULL;
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	// check if page(0 based) exists
	if (pageNum >= getNumberOfPages()) return -1;
	//read file at the page pos
    if(fseek(openedFile, pageNum * PAGE_SIZE, SEEK_SET) != 0) return -1;
    if(fread(data, sizeof(char), PAGE_SIZE, openedFile) != PAGE_SIZE) return -1;

    readPageCounter += 1;

    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	// check if page(0 based) exists
	if (pageNum >= getNumberOfPages()) return -1;
	//write
    if(fseek(openedFile, PAGE_SIZE * pageNum, SEEK_SET) != 0) return -1;
    if(fwrite(data, sizeof(char), PAGE_SIZE, openedFile) != PAGE_SIZE) return -1;
    if(fflush(openedFile) != 0) return -1;

    writePageCounter += 1;

    return 0;
}


RC FileHandle::appendPage(const void *data)
{
	fseek(openedFile, 0, SEEK_END);
	fwrite(data, sizeof(char), PAGE_SIZE, openedFile);
	fflush(openedFile);

    fileSize += PAGE_SIZE;
    appendPageCounter += 1;

    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    return fileSize/PAGE_SIZE;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}
