#include <Common/Compat.h>
#include <Common/Error.h>
#include <Common/System.h>

#include "TableInputFormat.h"

#include <Hypertable/Lib/Client.h>
#include "TableRangeMap.h"
#include <Hypertable/Lib/RangeLocationInfo.h>

#include <string>

using namespace Hypertable;
using namespace Mapreduce;
using namespace std;

namespace Mapreduce {

  extern "C" JNIEXPORT jobjectArray JNICALL
    Java_org_hypertable_mapreduce_TableInputFormat_getRangeVector
    (JNIEnv *env, jobject, jstring jtableName, jstring jrootPath)
    {
      string tableName;
      string rootPath;

      const char *raw_root_path =
        env->GetStringUTFChars(const_cast<jstring>(jrootPath), NULL);
      rootPath = raw_root_path;
      env->ReleaseStringUTFChars(jrootPath, raw_root_path);

      const char *raw_name = env->GetStringUTFChars(const_cast<jstring>(jtableName), NULL);
      tableName = raw_name;
      env->ReleaseStringUTFChars(jtableName, raw_name);

      TableRangeMap map(tableName, rootPath);

      vector<RangeLocationInfo> *vec;
      vector<RangeLocationInfo>::iterator it;

      vec = map.getMap();

      jclass stringClass = env->FindClass("Ljava/lang/String;");
      jobjectArray mappings = env->NewObjectArray(vec->size()*3, stringClass, NULL);
      jstring str = NULL;

      for (unsigned int k = 0; k < vec->size(); k++) {
        RangeLocationInfo range(vec->at(k));

        string start_row = range.start_row;
        string end_row = range.end_row;
        string location = range.location;

        str = env->NewStringUTF(start_row.c_str());
        env->SetObjectArrayElement(mappings, k*3, str);
        env->DeleteLocalRef(str);

        str = env->NewStringUTF(end_row.c_str());
        env->SetObjectArrayElement(mappings, k*3+1, str);
        env->DeleteLocalRef(str);

        str = env->NewStringUTF(location.c_str());
        env->SetObjectArrayElement(mappings, k*3+2, str);
        env->DeleteLocalRef(str);
      }

      delete vec;
      return mappings;
    }
}

