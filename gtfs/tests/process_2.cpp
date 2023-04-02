#include <dirent.h>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <vector>
#include <unistd.h>
#include <unordered_map> // used in gtfs_clean() to cache file descriptors
#include <pthread.h> // to use pthread mutex
#include <sys/ipc.h>
#include <sys/sem.h> // to use semaphore
#include <sys/shm.h> // to use shared memory
#include <string.h> // strcpy, strcmp
#include <sstream> // to concatenate multiple strings

using namespace std;

struct sembuf acquire_op, release_op;

static void initialize_semaphore(struct sembuf* acquire_op, struct sembuf* release_op) {
	acquire_op->sem_num = 0;
	acquire_op->sem_op = -1;
	acquire_op->sem_flg = SEM_UNDO;

	release_op->sem_num = 0;
	release_op->sem_op = 1;
	release_op->sem_flg = SEM_UNDO;
}


int main() {

    string filepath = "/home/shu335/project3/gtfs/tests/test2.txt";
    const char *pathname = filepath.c_str();

    int file_dscpt = open(pathname, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	if (file_dscpt == -1) {
		perror("Failed to open file");
		return 0;
	}
	// Attempt to acquire a lock on the file
	if (lockf(file_dscpt, F_TLOCK, 0) == -1) {
        perror("Failed to acquire file lock");
        close(file_dscpt);
        return 0;
    }

    // can still read?
//    char *start;
//    if (read(file_dscpt, start, 10*sizeof(char))>0) {
//        cout<<"can still read"<<endl;
//    }
//	else{
//	    cout<<"cannot"<<endl;
//	}

    return 1;
}

//int main() {
//
//    string directory("/home/shu335/project3/gtfs/src");
//    key_t dir_key = ftok(directory.c_str(), 1);
//    int sem_id = semget(dir_key, 1, 0666 | IPC_CREAT);
//
//    union semun {
//    int val;               // value for SETVAL
//    struct semid_ds *buf;  // buffer for IPC_STAT, IPC_SET
//    unsigned short *array; // array for GETALL, SETALL
//    };
//    union semun arg;
//
//    arg.val = 1;
//
////    if (semctl(sem_id, 0, SETVAL, arg) == -1) {
////        perror("semctl");
////        exit(1);
////    }
//
//	initialize_semaphore(&acquire_op, &release_op);
//	cout<<"sem_id is: "<< sem_id << endl;
//    semop(sem_id, &acquire_op, 1);
////    semop(sem_id, &acquire_op, 1);
//    cout<< "Acquired semaphore in process 2" << endl;
//    // Release the semaphore
//    semop(sem_id, &release_op, 1);
//
//    return 0;
//}
