#include <jni.h>
/* Header for class TableInputFormat */

namespace Mapreduce {
#ifndef _Included_TableInputFormat
#define _Included_TableInputFormat
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     TableInputFormat
 * Method:    getRangeVector
 * Signature: (Ljava/lang/String;)[Ljava/lang/String;
 */
JNIEXPORT jobjectArray JNICALL Java_org_hypertable_mapreduce_TableInputFormat_getRangeVector
  (JNIEnv *, jobject, jstring);

#ifdef __cplusplus
}
#endif
#endif
}

