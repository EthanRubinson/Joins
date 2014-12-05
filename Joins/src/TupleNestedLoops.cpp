#include "join.h"
#include "scan.h"

//---------------------------------------------------------------
// TupleNestedLoop::Execute
//
// Input:   left  - The left relation to join. 
//          right - The right relation to join. 
// Output:  out   - The relation to hold the ouptut. 
// Return:  OK if join completed succesfully. FAIL otherwise. 
//          
// Purpose: Performs a nested loop join on relations left and right
//          a tuple a time. You can assume that left is the outer
//          relation and right is the inner relation. 
//---------------------------------------------------------------
Status TupleNestedLoops::Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out) {
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

	//	Loop over the left relation
	char *leftRec = new char[left.recLen];
	while (true) {
		RecordID leftRid;
		leftStatus = leftScan->GetNext(leftRid, leftRec, left.recLen);
		if (leftStatus == DONE) break;
		if (leftStatus != OK) return FAIL;

		//	The join attribute on left relation
		int *leftJoinValPtr = (int*)(leftRec + left.offset);

		//	Open scan on right relation
		Status rightStatus;
		Scan *rightScan = right.file->OpenScan(rightStatus);
		if (rightStatus != OK) {
			std::cerr << "Failed to open scan on right relation." << std::endl;
			return FAIL;
		}

		//	Loop over right relation
		char *rightRec = new char[right.recLen];
		while (true) {
			RecordID rightRid;
			rightStatus = rightScan->GetNext(rightRid, rightRec, right.recLen);
			if (rightStatus == DONE) break;
			if (rightStatus != OK) return FAIL;

			//	Compare join attribute
			int *rightJoinValPtr = (int*)(rightRec + right.offset);
			if (*leftJoinValPtr == *rightJoinValPtr) {
				//	Create the record and insert into tmpHeap...
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

		delete [] rightRec;
		delete rightScan;
	}

	out.file = tmpHeap;
	delete leftScan;
	delete [] leftRec;

	return OK;
}