#include "../src/gtfs.hpp"

// Assumes files are located within the current directory
string directory;
int verbose;

// **Test 1**: Testing that data written by one process is then successfully read by another process.
void writer() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test1.txt";
    cout<< "before open" << endl;
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    cout<< "after open" << endl;

    string str = "Hi, I'm the writer.\n";
    write_t *wrt = gtfs_write_file(gtfs, fl, 10, str.length(), str.c_str());
    gtfs_sync_write_file(wrt);

    gtfs_close_file(gtfs, fl);
}

void reader() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test1.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Hi, I'm the writer.\n";
    char *data = gtfs_read_file(gtfs, fl, 10, str.length());
    if (data != NULL) {
        str.compare(string(data)) == 0 ? cout << PASS : cout << FAIL;
    } else {
        cout << FAIL;
    }
    gtfs_close_file(gtfs, fl);
}

void test_write_read() {
    int pid;
    pid = fork();
    if (pid < 0) {
        perror("fork");
        exit(-1);
    }
    if (pid == 0) {
        writer();
        exit(0);
    }
    waitpid(pid, NULL, 0);
    reader();
}

// **Test 2**: Testing that aborting a write returns the file to its original contents.
void test_abort_write() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test2.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);

    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    gtfs_abort_write_file(wrt2);

    char *data1 = gtfs_read_file(gtfs, fl, 0, str.length());
    if (data1 != NULL) {
        // First write was synced so reading should be successfull
        if (str.compare(string(data1)) != 0) {
            cout << FAIL;
        }
        // Second write was aborted and there was no string written in that offset
        char *data2 = gtfs_read_file(gtfs, fl, 20, str.length());
        if (data2 == NULL) {
            cout << FAIL;
        } else if (string(data2).compare("") == 0) {
            cout << PASS;
        }
    } else {
        cout << FAIL;
    }
    gtfs_close_file(gtfs, fl);
}

// **Test 3**: Testing that the logs are truncated.

void test_truncate_log() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test3.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);

    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    gtfs_sync_write_file(wrt2);

    cout << "Before GTFS cleanup\n";
    system("ls -l .");

    gtfs_clean(gtfs);

    cout << "After GTFS cleanup\n";
    system("ls -l .");

    cout << "If log is truncated: " << PASS << "If exactly same output:" << FAIL;

    gtfs_close_file(gtfs, fl);

}


// TODO: Implement any additional tests
// **Test 2**: Testing that abort a process for crash recovery.

void test_crash_recovery() {

    int pid;
    pid = fork();
    if (pid < 0){
        perror("fork");
        exit(-1);
    }
    if (pid==0){
        gtfs_t *gtfs = gtfs_init(directory, verbose);
        string filename = "test6.txt";
        file_t *fl = gtfs_open_file(gtfs, filename, 100);
        string str = "Testing string.\n";
        write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
        gtfs_sync_write_file(wrt1);
        write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
        gtfs_sync_write_file(wrt2);
        abort();
    }
    else if (pid >0){
        waitpid(pid, NULL, 0);
        gtfs_t *gtfs = gtfs_init(directory, verbose);
//        gtfs_clean(gtfs);
        string str = "Testing string.\n";
        string filename = "test6.txt";
        file_t *fl = gtfs_open_file(gtfs, filename, 100);
        char *data1 = gtfs_read_file(gtfs, fl, 0, str.length());
        char *data2 = gtfs_read_file(gtfs, fl, 20, str.length());
        if (data1 == NULL || data2 == NULL){
            cout << FAIL;
        } else if (str.compare(string(data1)) == 0 && str.compare(string(data2)) == 0){
            cout << PASS;
        } else {
            cout << FAIL;
        }
        gtfs_close_file(gtfs, fl);
    }
    return;
}


void test_partial_write() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test4.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string1. Testing string2. Testing string3.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file_n_bytes(wrt1, 20);

    gtfs_clean(gtfs);
    system("ls -l .");
    gtfs_close_file(gtfs, fl);
}

void test_partial_clean() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test5.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string1. Testing string2. Testing string3.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);

    gtfs_clean_n_bytes(gtfs, 20);
    system("ls -l .");
    gtfs_close_file(gtfs, fl);
}


int test_muti_process_access() {

    pid_t pid = fork();

    if (pid < 0) {
        // fork error
        cerr << "Error: fork failed." << endl;
        return 1;
    } else if (pid == 0) {
        // child process
        cout << "Child process running." << endl;
        string str_c = "Child test string.\n";

        gtfs_t *gtfs = gtfs_init(directory, verbose);

        string filename_c1 = "test10.txt";
        file_t *fl_c1 = gtfs_open_file(gtfs, filename_c1, 100);
        if (fl_c1){
            write_t *wrt_c1 = gtfs_write_file(gtfs, fl_c1, 0, str_c.length(), str_c.c_str());
            gtfs_sync_write_file(wrt_c1);
            cout << "Child process opened and write into test10.txt" << endl;
        }
        else cout << "Child process open test10.txt wrong.\n";

        sleep(1);

        string filename_c2 = "test11.txt";
        file_t *fl_c2 = gtfs_open_file(gtfs, filename_c2, 100);
        if (fl_c2){
            write_t *wrt_c2 = gtfs_write_file(gtfs, fl_c2, 0, str_c.length(), str_c.c_str());
            gtfs_sync_write_file(wrt_c2);
            cout << "Child process opened and write into test11.txt" << endl;
        }
        else cout << "Child process open test11.txt wrong.\n";

        // clean
        sleep(3);
        gtfs_clean(gtfs);
        system("ls -l .");

        return 0;
    } else {

        // Parent process
        cout << "Parent process running." << endl;
        string str_p = "Parent test string.\n";

        gtfs_t *gtfs = gtfs_init(directory, verbose);

        string filename_p1 = "test12.txt";
        file_t *fl_p1 = gtfs_open_file(gtfs, filename_p1, 100);
        if(fl_p1){
            write_t *wrt_p1 = gtfs_write_file(gtfs, fl_p1, 0, str_p.length(), str_p.c_str());
            gtfs_sync_write_file(wrt_p1);
            cout << "Parent process opened and write into test12.txt" << endl;
        }
        else cout << "Parent process open test12.txt wrong.\n";

        sleep(1);

        string filename_p2 = "test11.txt";
        file_t *fl_p2 = gtfs_open_file(gtfs, filename_p2, 100);
        if(fl_p2){
            write_t *wrt_p2 = gtfs_write_file(gtfs, fl_p2, 0, str_p.length(), str_p.c_str());
            gtfs_sync_write_file(wrt_p2);
            cout << "Parent process opened and write into test11.txt" << endl;
        }
        else cout << "Parent process open test11.txt wrong.\n";

        // clean
        gtfs_clean(gtfs);
        system("ls -l .");

        wait(NULL); // wait for child process to finish
        return 0;
    }
}


// creative test cases

int main(int argc, char **argv) {
    if (argc < 2)
        printf("Usage: ./test verbose_flag\n");
    else
        verbose = strtol(argv[1], NULL, 10);

    // Get current directory path
    char cwd[256];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        directory = string(cwd);
    } else {
        cout << "[cwd] Something went wrong.\n";
    }

    // Call existing tests
//    cout << "================== Test 1 ==================\n";
//    cout << "Testing that data written by one process is then successfully read by another process.\n";
//    test_write_read();
//
//    cout << "================== Test 2 ==================\n";
//    cout << "Testing that aborting a write returns the file to its original contents.\n";
//    test_abort_write();

//    cout << "================== Test 3 ==================\n";
//    cout << "Testing that the logs are truncated.\n";
//    test_truncate_log();

//    cout << "================== Test 4 ==================\n";
//    cout << "Testing that partial writes to redo log.\n";
//    test_partial_write();
//
//    cout << "================== Test 5 ==================\n";
//    cout << "Testing that partial writes to redo log.\n";
//    test_partial_clean();

//    cout << "================  Test 6 =============\n";
//    cout << "Testing crash recovery.\n";
//    test_crash_recovery();

    cout << "=============== Test 7 ======================\n";
    cout << "Testing multi process access."<<endl;
    test_muti_process_access();

    // TODO: Call any additional tests

}
