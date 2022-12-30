
#include <stdio.h>
#include <time.h>
#include <jni.h>


 jlong JNICALL Java_py_coordinator_utils_GetMicrosecondTimestamp_getMicrosecondTimestampByCLanguage
  (JNIEnv *env, jclass cla)
{
    struct timespec time = {0, 0};
    clock_gettime(CLOCK_REALTIME, &time);
    jlong current_time = ((jlong) time.tv_sec) * 1000000000 + time.tv_nsec;
    return current_time/1000;
}
