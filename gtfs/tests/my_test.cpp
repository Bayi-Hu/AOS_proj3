#include <unistd.h>
#include <iostream>
#include <sys/wait.h>

using namespace std;

int main() {
    pid_t pid = fork();

    if (pid < 0) {
        // fork error
        cerr << "Error: fork failed." << endl;
        return 1;
    } else if (pid == 0) {
        // child process
        cout << "Child process running." << endl;
        // do some work here
        sleep(1);
        cout << "Child 1" << endl;
        sleep(2);
        cout << "Child 2" << endl;
        sleep(3);
        cout << "Child 3" << endl;

        return 0;
    } else {
        // parent process
        cout << "Parent process running." << endl;
        // do some work here
        sleep(1);
        cout << "Parent 1" << endl;
        sleep(2);
        cout << "Parent 2" << endl;
        sleep(3);
        cout << "Parent 3" << endl;

        wait(NULL); // wait for child process to finish
        return 0;
    }
}

