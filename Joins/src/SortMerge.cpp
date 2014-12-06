#include "join.h"
#include "scan.h"


//---------------------------------------------------------------
// SortMerge::Execute
//
// Input:   left  - The left relation to join. 
//          right - The right relation to join. 
// Output:  out   - The relation to hold the ouptut. 
// Return:  OK if join completed succesfully. FAIL otherwise. 
//          
// Purpose: Performs an sort merge join on the specified relations. 
// Please see the pseudocode on page 460 of your text for more info
// on this algorithm. You may use JoinMethod::SortHeapFile to sort
// the relations. 
//---------------------------------------------------------------
Status SortMerge::Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out) {
	JoinMethod::Execute(left, right, out);
	
	HeapFile *leftSortedFile = SortHeapFile(left.file, left.recLen, left.offset);
	HeapFile *rightSortedFile = SortHeapFile(right.file, right.recLen, right.offset);

	//	Create the temporary heapfile
	Status st;
	HeapFile *tmpHeap = new HeapFile(NULL, st);
	if (st != OK) {
		std::cerr << "Failed to create output heapfile." << std::endl;
		return FAIL;
	}

	//	Open scan on left relation
	Status leftStatus;
	Scan *leftScan = leftSortedFile->OpenScan(leftStatus);
	if (leftStatus != OK) {
		std::cerr << "Failed to open scan on sorted left relation." << std::endl;
		return FAIL;
	}

	//	Open scan on right relation
	Status rightStatus;
	Scan *rightScan = rightSortedFile->OpenScan(rightStatus);
	if (rightStatus != OK) {
		std::cerr << "Failed to open scan on sorted right relation." << std::endl;
		return FAIL;
	}

	//  Open G scan on right relation 
	Status rightGStatus;
	Scan *rightGScan = rightSortedFile->OpenScan(rightGStatus);
	if (rightGStatus != OK) {
		std::cerr << "Failed to open G scan on sorted right relation." << std::endl;
		return FAIL;
	}

	char *leftRec   = new char[left.recLen];
	char *rightRec  = new char[right.recLen];
	char *rightGRec = new char[right.recLen];

	//  Scan the first tuple for each scan
	RecordID leftRid, rightRid, rightGRid;
	leftStatus   = leftScan->GetNext(leftRid, leftRec, left.recLen);
	rightStatus  = rightScan->GetNext(rightRid, rightRec, right.recLen);
	rightGStatus = rightGScan->GetNext(rightGRid, rightGRec, right.recLen);

	while (leftStatus != DONE && rightGStatus != DONE) {
		int *leftJoinValPtr = (int*)(leftRec + left.offset);
		int *rightGJoinValPtr = (int*)(rightGRec + right.offset);
			
		//  While leftRec < rightGRec, get the next tuple from the leftScan
		while (*leftJoinValPtr < *rightGJoinValPtr) {
			leftStatus = leftScan->GetNext(leftRid, leftRec, left.recLen);
			if (leftStatus == DONE) break;
			else if (leftStatus != OK) return FAIL;
			else leftJoinValPtr = (int*)(leftRec + left.offset);
		}

		if (leftStatus == DONE) break;
		
		//  While leftRec > rightGRec, get the next tuple from the rightGScan
		while (*leftJoinValPtr > *rightGJoinValPtr) {
			rightGStatus = rightGScan->GetNext(rightGRid, rightGRec, right.recLen);
			if (rightGStatus == DONE) break;
			else if (rightGStatus != OK) return FAIL;
			else rightGJoinValPtr = (int*)(rightGRec + right.offset);
		}

		if (rightGStatus == DONE) break;

		//  Reset [rightRec (Ts) = rightGRec (Gs)] the partition scan
		rightScan->MoveTo(rightGRid);
		rightStatus = rightScan->GetNext(rightRid, rightRec, right.recLen);
		if (rightStatus != OK) return FAIL;

		//  While leftRec == rightGRec
		while (*leftJoinValPtr == *rightGJoinValPtr) {

			//  Reset [rightRec (Ts) = rightGRec (Gs)] the partition scan
			rightScan->MoveTo(rightGRid);
			rightStatus = rightScan->GetNext(rightRid, rightRec, right.recLen);
			if (rightStatus != OK) return FAIL;

			int *rightJoinValPtr = (int*)(rightRec + right.offset);

			//  While rightRec == leftRec
			while (*rightJoinValPtr == *leftJoinValPtr) {
				char *joinedRec = new char[out.recLen];
				MakeNewRecord(joinedRec, leftRec, rightRec, left, right);
				RecordID insertedRid;
				Status tmpStatus =  tmpHeap->InsertRecord(joinedRec,out.recLen, insertedRid);

				if (tmpStatus != OK){
					std::cerr << "Failed to insert tuple into output heapfile." << std::endl;
					return FAIL;
				}
				delete [] joinedRec;

				rightStatus = rightScan->GetNext(rightRid, rightRec, right.recLen);

				if (rightStatus == DONE) break;
				else if (rightStatus != OK) return FAIL;
			}

			leftStatus = leftScan->GetNext(leftRid, leftRec, left.recLen);

			if (leftStatus == DONE)	break;
			else if (leftStatus != OK) return FAIL;
		}

		rightGScan->MoveTo(rightRid);
		rightGStatus = rightGScan->GetNext(rightGRid, rightGRec, right.recLen);
	}

	out.file = tmpHeap;

	delete leftScan;
	delete rightScan;

	delete [] leftRec;
	delete [] rightRec;
	delete [] rightGRec;

	delete leftSortedFile;
	delete rightSortedFile;

	return OK;
}