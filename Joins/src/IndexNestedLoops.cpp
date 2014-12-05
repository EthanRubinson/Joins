#include "join.h"
#include "scan.h"
#include "bufmgr.h"
#include "BTreeFile.h"
#include "BTreeFileScan.h"


//---------------------------------------------------------------
// IndexNestedLoops::Execute
//
// Input:   left  - The left relation to join. 
//          right - The right relation to join. 
// Output:  out   - The relation to hold the ouptut. 
// Return:  OK if join completed succesfully. FAIL otherwise. 
//          
// Purpose: Performs an index nested loops join on the specified relations. 
// You should create a BTreeFile index on the join attribute of the right 
// relation, and then probe it for each record in the left relation. Remember 
// that the BTree expects string keys, so you will have to convert the integer
// attributes to a string using JoinMethod::toString. Note that the join may 
// not be a foreign key join, so there may be multiple records indexed by the 
// same key. Good thing our B-Tree supports this! Don't forget to destroy the 
// BTree when you are done. 
//---------------------------------------------------------------
Status IndexNestedLoops::Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out) {
	JoinMethod::Execute(left, right, out);

	//	Create the temporary heapfile
	Status st;
	HeapFile *tmpHeap = new HeapFile(NULL, st);
	if (st != OK) {
		std::cerr << "Failed to create output heapfile." << std::endl;
		return FAIL;
	}

	//	Open scan on left relation
	Status leftStatus;
	Scan *leftScan = left.file->OpenScan(leftStatus);
	if (leftStatus != OK) {
		std::cerr << "Failed to open scan on left relation." << std::endl;
		return FAIL;
	}

	//	Open scan on right relation
	Status rightStatus;
	Scan *rightScan = right.file->OpenScan(rightStatus);
	if (rightStatus != OK) {
		std::cerr << "Failed to open scan on right relation." << std::endl;
		return FAIL;
	}

	//  Create the unclustered B+ tree index
	Status treeStatus;
	BTreeFile *tree = new BTreeFile(treeStatus, "BPlusTreeIndex");
	if (treeStatus != OK) {
		std::cerr << "Failed to create B+ tree index." << std::endl;
		return FAIL;
	}

	//  Populate the tree with the records from the right relation
	char *rightKey = new char[9];
	char *rightRec  = new char[right.recLen];
	while (true){
		RecordID rightRid;
		rightStatus = rightScan->GetNext(rightRid, rightRec, right.recLen);
		if (rightStatus == DONE) break;
		if (rightStatus != OK) return FAIL;

		//	The join attribute on right relation
		int *rightJoinValPtr = (int*)(rightRec + right.offset);

		toString(*rightJoinValPtr, rightKey);

		if (tree->Insert(rightKey, rightRid) != OK) return FAIL;
	}

	//  Search the B+ tree index for the keys in the left relation
	char *leftKey = new char[9];
	char *leftRec = new char[left.recLen];
	while (true){
		RecordID leftRid;
		leftStatus = leftScan->GetNext(leftRid, leftRec, left.recLen);
		if (leftStatus == DONE) break;
		if (leftStatus != OK) return FAIL;

		//	The join attribute on left relation
		int *leftJoinValPtr = (int*)(leftRec + left.offset);

		toString(*leftJoinValPtr, leftKey);

		//  Scan the B+ tree index for matches on the leftKey
		BTreeFileScan *btScan = tree->OpenScan(leftKey, leftKey);
		while (true){
			char *tmpKey = NULL;
			RecordID rid;
			Status btreeStatus = btScan->GetNext(rid, tmpKey);
			if (btreeStatus == DONE) break;
			if (btreeStatus != OK) return FAIL;

			right.file->GetRecord(rid, rightRec, right.recLen);

			char *joinedRec = new char[out.recLen];
			MakeNewRecord(joinedRec, leftRec, rightRec, left, right);
			RecordID insertedRid;
			Status tmpStatus = tmpHeap->InsertRecord(joinedRec, out.recLen, insertedRid);

			if (tmpStatus != OK) {
				std::cerr << "Failed to insert tuple into output heapfile." << std::endl;
				return FAIL;
			}
					
			delete [] joinedRec;
		}
	}

	out.file = tmpHeap;
	
	delete leftScan;
	delete rightScan;

	delete [] leftRec;
	delete leftKey;

	delete [] rightRec;
	delete rightKey;

	tree->DestroyFile();
	delete tree;

	return OK;
}