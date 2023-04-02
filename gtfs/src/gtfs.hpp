#ifndef GTFS
#define GTFS

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

#define PASS "\033[32;1m PASS \033[0m\n"
#define FAIL "\033[31;1m FAIL \033[0m\n"

// GTFileSystem basic data structures

#define MAX_FILENAME_LEN 255
#define MAX_NUM_FILES_PER_DIR 1024
#define MAX_LOG_SIZE 1024

extern int do_verbose;

typedef struct gtfs {
    string dirname;
    int sem_id; // use semaphore to synchronize write
} gtfs_t;

typedef struct file {
    string filename;
	int file_length;
    int file_dscpt; // file descriptor
    int log_dscpt; // its file log descriptor
    char* data; // in-memory version of data
} file_t;

typedef struct write {
    string filename;
	int offset;
	int length;
	char *data;
    char *undo_data; // undo record
    int sem_id;
    int log_dscpt; // descriptor for log file
    file_t* file; // to manipulate in-memory version of data
} write_t;

// GTFileSystem basic API calls

gtfs_t* gtfs_init(string directory, int verbose_flag);
int gtfs_clean(gtfs_t *gtfs);

file_t* gtfs_open_file(gtfs_t* gtfs, string filename, int file_length);
int gtfs_close_file(gtfs_t* gtfs, file_t* fl);
int gtfs_remove_file(gtfs_t* gtfs, file_t* fl);

char* gtfs_read_file(gtfs_t* gtfs, file_t* fl, int offset, int length);
write_t* gtfs_write_file(gtfs_t* gtfs, file_t* fl, int offset, int length, const char* data);
int gtfs_sync_write_file(write_t* write_id);
int gtfs_abort_write_file(write_t* write_id);

// TODO: Add here any additional data structures or API calls
int gtfs_clean_n_bytes(gtfs *gtfs, int bytes);
int gtfs_sync_write_file_n_bytes(write_t* write_id, int bytes);

#endif
