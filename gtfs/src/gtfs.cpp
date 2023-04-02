#include "gtfs.hpp"

#define VERBOSE_PRINT(verbose, str...) do { \
	if (verbose) cout << "VERBOSE: "<< __FILE__ << ":" << __LINE__ << ":" << __func__ << "():" << str; \
	} while(0)

int do_verbose;
struct sembuf acquire_op, release_op;

//static void initialize_semaphore(struct sembuf* acquire_op, struct sembuf* release_op) {
//	acquire_op->sem_num = 0;
//	acquire_op->sem_op = -1;
//	acquire_op->sem_flg = 0;
//
//	release_op->sem_num = 0;
//	release_op->sem_op = 1;
//	release_op->sem_flg = 0;
//}

gtfs_t* gtfs_init(string directory, int verbose_flag) {
	do_verbose = verbose_flag;
	gtfs_t *gtfs = NULL;
	VERBOSE_PRINT(do_verbose, "Initializing GTFileSystem inside directory " << directory << "\n");

    cout<< "the directory is :"<< directory << endl;
	// Obtain the shared memory ID and semaphore ID for the metadata
	key_t dir_key = ftok(directory.c_str(), 1);
	int sem_id = semget(dir_key, 1, 0666 | IPC_CREAT);

	gtfs = new gtfs_t;
	gtfs->dirname = directory;
	gtfs->sem_id = sem_id;
//	cout<< "the semaphore id is "<<sem_id << endl;

//	initialize_semaphore(&acquire_op, &release_op);

	VERBOSE_PRINT(do_verbose, "Success.\n"); //On success returns non NULL.
	std::cout << "GTFileSystem initialized successfully\n";
	return gtfs;
}

int gtfs_clean(gtfs_t *gtfs) {
	int ret = -1;
	if (gtfs) {
		VERBOSE_PRINT(do_verbose, "Cleaning up GTFileSystem inside directory " << gtfs->dirname << "\n");
	} else {
		VERBOSE_PRINT(do_verbose, "GTFileSystem is not existed\n");
		return ret;
	}
	// Lock the semaphore
    // semop(gtfs->sem_id, &acquire_op, 1);

    // Go through all the gtfs log, apply them to the corresponding files
    DIR* dir = opendir(gtfs->dirname.c_str());
    dirent* entry;
    string suffix = ".log";

    while ((entry = readdir(dir)) != NULL) {
        string entry_name(entry->d_name);

        if (entry_name == "." || entry_name == "..") continue; // Skip "." and ".."
        bool is_log = (entry_name.length() >= suffix.length()) &&
                      (entry_name.substr(entry_name.length() - suffix.length()) == suffix);

        if (!is_log) continue;
        string log_name = entry_name;
        string file_name = log_name.substr(0, log_name.length() - suffix.length());

        log_name = gtfs->dirname + "/" + log_name;
        file_name = gtfs->dirname + "/" + file_name;

        cout<<"log name is:"<< log_name<<endl;
        cout<<"file name is:"<< file_name<<endl;

        // Open the file
        int file_dscpt = open(file_name.c_str(), O_RDWR);
        if (file_dscpt == -1) {
            VERBOSE_PRINT(do_verbose, "Error opening file " << file_name << "\n");
            continue;
        }
        // Attempt to acquire a lock on the file
        if (lockf(file_dscpt, F_TLOCK, 0) == -1){
            perror("Failed to acquire file lock"); // means the lock is acquired by other processes.
            close(file_dscpt);
            continue; // move to the next log
        }

        // Map the file into memory and apply the changes
        struct stat statbuf;
        if (fstat(file_dscpt, &statbuf) == -1) {
            VERBOSE_PRINT(do_verbose, "Error getting file size for " << file_name << "\n");
            close(file_dscpt);
            continue;
        }
        char* mapped_data = (char*) mmap(NULL, statbuf.st_size, PROT_WRITE, MAP_SHARED, file_dscpt, 0);
        if (mapped_data == MAP_FAILED) {
            VERBOSE_PRINT(do_verbose, "Error mapping file " << file_name << " to memory\n");
            close(file_dscpt);
            continue;
        }

        // get the log descriptor based on the log name
        int log_dscpt = open(log_name.c_str(), O_RDWR);

        // Rewind the log file
	    lseek(log_dscpt, 0, SEEK_SET);

	    // Read and apply changes from the log file
	    int start;
	    while (read(log_dscpt, &start, sizeof(int))>0){

            // Read the offset, data length, and data from the log
            int offset, data_length;
            read(log_dscpt, &offset, sizeof(int));
            read(log_dscpt, &data_length, sizeof(int));
            char* data = new char[data_length];
            read(log_dscpt, data, data_length);
            memcpy(mapped_data + offset, data, data_length);
        }

        cout<<"here"<<endl;
        munmap(mapped_data, statbuf.st_size);
        fsync(file_dscpt);
        close(file_dscpt);

	    truncate(log_name.c_str(), 0);
	}

	// Unlock the semaphore
    // semop(gtfs->sem_id, &release_op, 1);

	VERBOSE_PRINT(do_verbose, "Success.\n"); //On success returns 0.
	ret = 0;
	return ret;
}

file_t* gtfs_open_file(gtfs_t* gtfs, string filename, int file_length) {
	file_t *fl = NULL;
	if (gtfs) {
		VERBOSE_PRINT(do_verbose, "Opening file " << filename << " inside directory " << gtfs->dirname << "\n");
	} else {
		VERBOSE_PRINT(do_verbose, "GTFileSystem is not existed\n");
		return NULL;
	}

    string filepath = gtfs->dirname + "/" + filename;
    const char* pathname = filepath.c_str();

	int file_dscpt = open(pathname, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	if (file_dscpt == -1) {
		perror("Failed to open file");
		return fl;
	}
	// Attempt to acquire a lock on the file
	if (lockf(file_dscpt, F_TLOCK, 0) == -1) {
        perror("Failed to acquire file lock");
        close(file_dscpt);
        return nullptr;
    }

    // If file_length is smaller than the actual file size, fail the operation
    struct stat statbuf;
    if (stat(pathname, &statbuf) == -1) {
        perror("Failed to retrieve file stats");
        close(file_dscpt);
        return nullptr;
    }
    if (file_length < statbuf.st_size) {
        perror("Cannot truncate file to smaller size");
        close(file_dscpt);
        return nullptr;
    }
    // If file_length is greater than the actual file size, adjust the file size
    if (file_length > statbuf.st_size) {
        truncate(filepath.c_str(), file_length);
    }

    string log_path = gtfs->dirname + "/" + filename + ".log";
    const char* log_path_name = log_path.c_str();

	int log_dscpt = open(log_path_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	if (log_dscpt == -1) {
		perror("Failed to open the log file");
		return fl;
	}

    // Create a new file_t object to store the file information
	fl = new file_t;
	fl->filename = gtfs->dirname + "/" + filename; // store the absolute file name
	fl->file_dscpt = file_dscpt;
	fl->file_length = file_length;
	fl->data = new char[file_length];
	fl->log_dscpt = log_dscpt;

	read(file_dscpt, fl->data, file_length);

	// Apply the updates in the log file to the in-memory version of the data
    // semop(gtfs->sem_id, &acquire_op, 1);

    off_t start_pos = lseek(log_dscpt, 0, SEEK_SET);
    if (start_pos == -1){
        perror("In lseek() in gtfs_open_file");
    }

    int start;
    while (read(log_dscpt, &start, sizeof(int)) > 0) {

        int offset, data_len;
        read(log_dscpt, &offset, sizeof(int));
        read(log_dscpt, &data_len, sizeof(int));
        char* data = new char[data_len];
        read(log_dscpt, data, data_len);

        // apply changes on the file in-memory data
        memcpy(fl->data + offset, data, data_len);
    }

    // semop(gtfs->sem_id, &release_op, 1);

	VERBOSE_PRINT(do_verbose, "Success.\n"); //On success returns non NULL.
	return fl;
}

int gtfs_close_file(gtfs_t* gtfs, file_t* fl) {
	int ret = -1;
	if (gtfs and fl) {
		VERBOSE_PRINT(do_verbose, "Closing file " << fl->filename << " inside directory " << gtfs->dirname << "\n");
	} else {
		VERBOSE_PRINT(do_verbose, "GTFileSystem or file is not existed\n");
		return ret;
	}
	// Release the file lock and close the file descriptor
    ret = lockf(fl->file_dscpt, F_ULOCK, 0);
    if (ret == -1) {
        VERBOSE_PRINT(do_verbose, "Error releasing lock for file " << fl->filename << "\n");
        return -1;
    }
    ret = close(fl->file_dscpt);
    if (ret == -1) {
        VERBOSE_PRINT(do_verbose, "Error closing file " << fl->filename << "\n");
        return -1;
    }
    // Close the log file descriptor
    // semop(gtfs->sem_id, &acquire_op, 1);
    int ret_log = close(fl->log_dscpt);
    if (ret_log == -1) {
        VERBOSE_PRINT(do_verbose, "Error closing log file for GTFileSystem " << gtfs->dirname << "\n");
        return -1;
    }
    // semop(gtfs->sem_id, &release_op, 1);
	VERBOSE_PRINT(do_verbose, "File " << fl->filename << " closed successfully.\n"); //On success returns 0.
	return ret;
}

int gtfs_remove_file(gtfs_t* gtfs, file_t* fl) {
	int ret = -1;
	if (gtfs and fl) {
	    VERBOSE_PRINT(do_verbose, "Removing file " << fl->filename << " inside directory " << gtfs->dirname << "\n");
	} else {
	    VERBOSE_PRINT(do_verbose, "GTFileSystem or file is not existed\n");
	    return ret;
	}

	// Acquire semaphore so that the operation cannot be performed when the file is open
	stringstream ss1;
    ss1 << gtfs->dirname << "/" << fl->filename;
    string filepath = ss1.str();
    // semop(gtfs->sem_id, &acquire_op, 1);

    // Remove the file
    if (remove(filepath.c_str()) == -1) {
        perror("In gtfs_remove_file");
        return ret;
    }

    // Traverse the log file to invalidate all the log records related to this file
	string logpath = gtfs->dirname + "/log";
	int log_dscpt = open(logpath.c_str(), O_RDWR);
	struct stat statbuf;
	stat(logpath.c_str(), &statbuf);
	int log_length = statbuf.st_size;
	char* log_data = (char*) mmap(NULL, log_length, PROT_WRITE, MAP_SHARED, log_dscpt, 0);

	if (log_data == MAP_FAILED) {
        perror("In gtfs_remove_file(), when mapping log data");
        close(log_dscpt);
    //  semop(gtfs->sem_id, &release_op, 1);
        return ret;
    }

	int pos = 0;

	while (pos < log_length) {
		int filename_length, offset, data_length;
		memcpy(&filename_length, log_data + pos, sizeof(int));
		pos += sizeof(int);
		char* fname = new char[MAX_FILENAME_LEN];
		memcpy(fname, log_data + pos, filename_length);
		fname[filename_length] = '\0';
		pos += filename_length;
		memcpy(&offset, log_data + pos, sizeof(int));
		pos += sizeof(int);
		memcpy(&data_length, log_data + pos, sizeof(int));
		pos += sizeof(int);
		char* data = new char[data_length];
		memcpy(data, log_data + pos, data_length);
        pos += data_length;
        delete[] fname;
	}
	if (munmap(log_data, log_length) == -1) {
        perror("In gtfs_remove_file(), when unmapping log data");
        close(log_dscpt);
    //  semop(gtfs->sem_id, &release_op, 1);
        return ret;
    }

	fsync(log_dscpt);
	close(log_dscpt);

	// Free memory and release semaphore
	delete[] fl->data;
	delete fl;
    // semop(gtfs->sem_id, &release_op, 1);

	VERBOSE_PRINT(do_verbose, "Success.\n"); //On success returns 0.
	ret = 0;
	return ret;
}

char* gtfs_read_file(gtfs_t* gtfs, file_t* fl, int offset, int length) {
	char* ret_data = NULL;
	if (gtfs and fl) {
		VERBOSE_PRINT(do_verbose, "Reading " << length << " bytes starting from offset " << offset << " inside file " << fl->filename << "\n");
	} else {
		VERBOSE_PRINT(do_verbose, "GTFileSystem or file is not existed\n");
		return NULL;
	}

	// Allocate memory for the data to be read from the file
	ret_data = new char[length];

	// Copy the requested data from the file buffer to the allocated memory
    memcpy(ret_data, fl->data + offset, length);

	VERBOSE_PRINT(do_verbose, "Success.\n"); //On success returns pointer to data read.
	// Return a pointer to the data read
	return ret_data;
}

write_t* gtfs_write_file(gtfs_t* gtfs, file_t* fl, int offset, int length, const char* data) {
	write_t *write_id = NULL;
	if (gtfs and fl) {
		VERBOSE_PRINT(do_verbose, "Writting " << length << " bytes starting from offset " << offset << " inside file " << fl->filename << "\n");
	} else {
		VERBOSE_PRINT(do_verbose, "GTFileSystem or file is not existed\n");
		return NULL;
	}

	write_id = new write_t;

	// Copy data and create undo data
	write_id->data = new char[length];
	write_id->undo_data = new char[length];
	memcpy(write_id->data, data, length);
	memcpy(write_id->undo_data, fl->data + offset, length);

    // Update file data
	memcpy(fl->data + offset, write_id->data, length);

	// Save metadata for undo
	write_id->filename = fl->filename; // absolute file name
	write_id->offset = offset;
	write_id->length = length;
	write_id->file = fl;
	write_id->sem_id = gtfs->sem_id;
	write_id->log_dscpt = fl->log_dscpt;

	VERBOSE_PRINT(do_verbose, "Success.\n"); //On success returns non NULL.

	return write_id;
}


int gtfs_sync_write_file(write_t* write_id) {
	int ret = -1;
	if (write_id) {
		VERBOSE_PRINT(do_verbose, "Persisting write of " << write_id->length << " bytes starting from offset " << write_id->offset << " inside file " << write_id->filename << "\n");
	} else {
		VERBOSE_PRINT(do_verbose, "Write operation is not exist\n");
		return ret;
	}

	// Obtain a lock on the semaphore
    // semop(write_id->sem_id, &acquire_op, 1);

    // Write the metadata to the log file
    int start = 1;
    write(write_id->log_dscpt, &start, sizeof(int));
    write(write_id->log_dscpt, &(write_id->offset), sizeof(int));
    write(write_id->log_dscpt, &(write_id->length), sizeof(int));
    write(write_id->log_dscpt, write_id->data, write_id->length);

    // Release the lock on the semaphore
    // semop(write_id->sem_id, &release_op, 1);

    // Free memory used by write_id
    delete write_id;

	VERBOSE_PRINT(do_verbose, "Success.\n"); //On success returns number of bytes written.
	return ret;
}

int gtfs_abort_write_file(write_t* write_id) {
	int ret = -1;
	if (write_id) {
		VERBOSE_PRINT(do_verbose, "Aborting write of " << write_id->length << " bytes starting from offset " << write_id->offset << " inside file " << write_id->filename << "\n");
	} else {
		VERBOSE_PRINT(do_verbose, "Write operation is not exist\n");
		return ret;
	}
	//TODO: Any additional initializations and checks
	char* file_data = write_id->file->data;
	memcpy(file_data + write_id->offset, write_id->undo_data, write_id->length);
	delete write_id;

	VERBOSE_PRINT(do_verbose, "Success.\n"); //On success returns 0.
	return ret;
}

int gtfs_clean_n_bytes(gtfs_t *gtfs, int bytes) {
    /*
    this function will clean only given number of bytes instead of the entire length of redo record.
    This is used to simulate a partial write of redo log to the original file before an abort or crash.
    */
	int ret = -1;
	if (gtfs) {
		VERBOSE_PRINT(do_verbose, "Cleaning up GTFileSystem inside directory " << gtfs->dirname << "\n");
	} else {
		VERBOSE_PRINT(do_verbose, "GTFileSystem is not existed\n");
		return ret;
	}
	// Lock the semaphore
    // semop(gtfs->sem_id, &acquire_op, 1);

    // Go through all the gtfs log, apply them to the corresponding files
    DIR* dir = opendir(gtfs->dirname.c_str());
    dirent* entry;
    string suffix = ".log";

    while ((entry = readdir(dir)) != NULL) {
        string entry_name(entry->d_name);

        if (entry_name == "." || entry_name == "..") continue; // Skip "." and ".."
        bool is_log = (entry_name.length() >= suffix.length()) &&
                      (entry_name.substr(entry_name.length() - suffix.length()) == suffix);

        if (!is_log) continue;
        string log_name = entry_name;
        string file_name = log_name.substr(0, log_name.length() - suffix.length());

        log_name = gtfs->dirname + "/" + log_name;
        file_name = gtfs->dirname + "/" + file_name;

        cout<<"log name is:"<< log_name<<endl;
        cout<<"file name is:"<< file_name<<endl;

        // Open the file
        int file_dscpt = open(file_name.c_str(), O_RDWR);
        if (file_dscpt == -1) {
            VERBOSE_PRINT(do_verbose, "Error opening file " << file_name << "\n");
            continue;
        }
        // Attempt to acquire a lock on the file
        if (lockf(file_dscpt, F_TLOCK, 0) == -1){
            perror("Failed to acquire file lock"); // means the lock is acquired by other processes.
            close(file_dscpt);
            continue; // move to the next log
        }

        // Map the file into memory and apply the changes
        struct stat statbuf;
        if (fstat(file_dscpt, &statbuf) == -1) {
            VERBOSE_PRINT(do_verbose, "Error getting file size for " << file_name << "\n");
            close(file_dscpt);
            continue;
        }
        char* mapped_data = (char*) mmap(NULL, statbuf.st_size, PROT_WRITE, MAP_SHARED, file_dscpt, 0);
        if (mapped_data == MAP_FAILED) {
            VERBOSE_PRINT(do_verbose, "Error mapping file " << file_name << " to memory\n");
            close(file_dscpt);
            continue;
        }

        // get the log descriptor based on the log name
        int log_dscpt = open(log_name.c_str(), O_RDWR);

        // Rewind the log file
	    lseek(log_dscpt, 0, SEEK_SET);

	    // Read and apply changes from the log file
	    int start;
	    while (read(log_dscpt, &start, sizeof(int))>0){

            // Read the offset, data length, and data from the log
            int offset, data_length;
            read(log_dscpt, &offset, sizeof(int));
            read(log_dscpt, &data_length, sizeof(int));
            char* data = new char[data_length];
            read(log_dscpt, data, data_length);

            if (bytes > data_length){
                VERBOSE_PRINT(do_verbose, "clean bytes exceeds the length, set bytes to length\n");
                bytes = data_length;
            }

            memcpy(mapped_data + offset, data, bytes);
        }

        cout<<"here"<<endl;
        munmap(mapped_data, statbuf.st_size);
        fsync(file_dscpt);
        close(file_dscpt);

	    truncate(log_name.c_str(), 0);
	}

	// Unlock the semaphore
    // semop(gtfs->sem_id, &release_op, 1);

	VERBOSE_PRINT(do_verbose, "Success.\n"); //On success returns 0.
	ret = 0;
	return ret;
}



int gtfs_sync_write_file_n_bytes(write_t* write_id, int bytes) {
    /*
    Similar to gtfs_sync_write_file. However, this function will sync only the given number of bytes instead of the entire length of the write.
    This is used to simulate a partial write to redo log before an abort or crash.
    */
	int ret = -1;
	if (write_id) {
		VERBOSE_PRINT(do_verbose, "Persisting write of " << write_id->length << " bytes starting from offset " << write_id->offset << " inside file " << write_id->filename << "\n");
	} else {
		VERBOSE_PRINT(do_verbose, "Write operation is not exist\n");
		return ret;
	}

    if (bytes > write_id->length){
        VERBOSE_PRINT(do_verbose, "Write bytes exceeds the length, set bytes to length\n");
        bytes = write_id->length;
    }

    // Write the metadata to the log file
    int start = 1;
    write(write_id->log_dscpt, &start, sizeof(int));
    write(write_id->log_dscpt, &(write_id->offset), sizeof(int));
    write(write_id->log_dscpt, &(bytes), sizeof(int));
    write(write_id->log_dscpt, write_id->data, bytes);

    // Release the lock on the semaphore
    // semop(write_id->sem_id, &release_op, 1);

    // Free memory used by write_id
    delete write_id;

	VERBOSE_PRINT(do_verbose, "Success.\n"); //On success returns number of bytes written.
	return ret;
}
