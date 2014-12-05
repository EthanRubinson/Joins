#include "join.h"
#include "scan.h"


//---------------------------------------------------------------
// BlockNestedLoop::Execute
//
// Input:   left  - The left relation to join. 
//          right - The right relation to join. 
// Output:  out   - The relation to hold the ouptut. 
// Return:  OK if join completed succesfully. FAIL otherwise. 
//          
// Purpose: Performs a block nested loops join on the specified relations. 
// You can find a specification of this algorithm on page 455. You should 
// choose the smaller of the two relations to be the outer relation, but you 
// should make sure to concatenate the tuples in order <left, right> when 
// producing output. The block size can be specified in the constructor, 
// and is stored in the variable blockSize. 
//---------------------------------------------------------------
Status BlockNestedLoops::Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out) {
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

	/*  Simulate a "block" with an array for storing records.
	 *  The amount of memory required is the size of a record
	 *  times the amount of records that can fit in a block
	 *
	 *  Since we can assume the left relation has <= the number
	 *  of values than the right, we want to use this block to
	 *  store the records of the left relation
	 */
	char *block = new char[left.recLen * blockSize];
	int recordsInBlock = 0;
	
	RecordID firstRightScanRid = rightScan->currRid;
	leftStatus = DONE;
	char *leftRec  = new char[left.recLen];
	while (true) {
		//  For each block b in R, read block b into an array
		while (recordsInBlock < blockSize) {
			RecordID leftRid;
			leftStatus = leftScan->GetNext(leftRid, leftRec, left.recLen);
			if (leftStatus == OK) {
				memcpy(block + (left.recLen * recordsInBlock), leftRec, left.recLen);
				recordsInBlock++;
			}
			else if (leftStatus == DONE) break;
			else return FAIL;
		}

		//  For each tuple s in S
		RecordID rightRid;
		char *rightRec = new char[right.recLen];
		while (true) {
			rightStatus = rightScan->GetNext(rightRid, rightRec, right.recLen);
			if (rightStatus == DONE) break;
			if (rightStatus != OK) return FAIL;

			int *rightJoinValPtr = (int*)(rightRec + right.offset);
			//  For each tuple r in b
			for (int index = 0; index < recordsInBlock; index++) {
				char *leftRec = (block + (left.recLen * index));
				int *leftJoinValPtr = (int*)(leftRec + left.offset);
				
				//  Match r with s
				if (*leftJoinValPtr == *rightJoinValPtr) {
					//  If Match then Insert (r,s)
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
		}
		delete [] rightRec;

		if (leftStatus == DONE) break;

		rightScan->MoveTo(firstRightScanRid);
		recordsInBlock = 0;
	}

	out.file = tmpHeap;

	delete [] leftRec;
	delete [] block;
	delete leftScan;
	delete rightScan;

	return OK;
}