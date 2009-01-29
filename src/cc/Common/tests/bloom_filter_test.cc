#include "Common/Compat.h"

#include <fstream>
#include <set>
#include <vector>

#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/device/null.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/line.hpp>
#include <boost/iostreams/copy.hpp>

#include "Common/System.h"
#include "Common/Serialization.h"
#include "Common/Sweetener.h"
#include "Common/BloomFilter.h"
#include "Common/Logger.h"

using namespace std;
using namespace Hypertable;

namespace {
class line_str_inserter : public boost::iostreams::line_filter {
public:
  explicit line_str_inserter(vector<string> *v) {
    vec = v;
  }
private:
  std::string do_filter(const std::string& line) {
    vec->push_back(line);
    return line;
  }
  vector<string> *vec;
};

void read_file(const string f, vector<string>& bufvec) {
  boost::iostreams::filtering_istream fin;
  ifstream fil(f.c_str(), ios_base::in | ios_base::binary);
  line_str_inserter line_inserter(&bufvec);

  fin.push(line_inserter);
  fin.push(boost::iostreams::gzip_decompressor());
  fin.push(fil);

  boost::iostreams::copy(fin, boost::iostreams::null_sink());
}

string reverse(string s) {
  char c;
  unsigned int sz = s.length();
  for (unsigned int i = 0; i < (sz / 2); i++) {
    c = s[i]; s[i] = s[sz - i - 1]; s[sz - i - 1] = c;
  }
  return s;
}

void generate_outliers(const vector<string>& words, vector<string>& outliers) {
  foreach (string s, words) {
    if (s != reverse(s)) {
      outliers.push_back(s + reverse(s));
      outliers.push_back(s + s);
      outliers.push_back(reverse(s) + s + reverse(s));
    }
  }
  sort(outliers.begin(), outliers.end());

  set<string> set1, set2;
  copy(words.begin(), words.end(), inserter(set1, set1.begin()));
  copy(outliers.begin(), outliers.end(), inserter(set2, set2.begin()));  

  vector<string> common_members;
  set_intersection(set1.begin(), set1.end(),
                   set2.begin(), set2.end(),
                   back_inserter(common_members));
  foreach (string common_str, common_members) {
    vector<string>::iterator it = find(outliers.begin(), outliers.end(), common_str);
    if (it != outliers.end()) {
      outliers.erase(it);
    }
  }
  
}

void test_bloom_filter() {
  vector<string> words;
  vector<string> outliers;

  read_file("words.gz", words);
  generate_outliers(words, outliers);
  
  BloomFilter filter(words.size(), 0.01);

  int i = 0;
  foreach (string word, words) {
    filter.insert(word);
    i++;
  }

  foreach (string word, words) {
    HT_EXPECT(filter.may_contain(word) == true, -1);
  }

  float false_positive = 0.0;
  foreach (string non_member, outliers) {
    if (filter.may_contain(non_member)) {
      false_positive++;
    }
  }

  HT_INFO_OUT << "False positive rate: expected 0.01, got "
              << (false_positive / (words.size() + outliers.size())) << HT_END;
  
}

}

int main(int argc, char *argv[]) {
  System::initialize(argv[0]);
  try {
    test_bloom_filter();
  }
  catch (Exception &e) {
    HT_FATAL_OUT << e << HT_END;
  }
  return 0;
}
