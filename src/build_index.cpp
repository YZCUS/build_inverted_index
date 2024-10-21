#include <iostream>
#include <fstream>
#include <string>
#include <zlib.h>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <queue>
#include "archive.h"
#include "archive_entry.h"
#include <regex>

const int CHUNK_SIZE = 1024 * 64;              // 64KB
const int LINES_PER_BATCH = 100000;            // process lines per batch
const std::string TEMP_DIR = "temp_index";     // temp directory
const size_t MEMORY_LIMIT = 800 * 1024 * 1024; // 800MB, leave space for lexicon and other operations

// forward declarations
struct Posting;
struct LexiconInfo;
struct IndexEntry;
struct CompareIndexEntry;

// estimate memory usage
size_t estimateMemoryUsage(const std::unordered_map<int, std::vector<std::pair<int, int>>> &index,
                           const std::unordered_map<std::string, LexiconInfo> &lexicon,
                           const std::unordered_map<int, std::string> &term_id_to_word,
                           const std::unordered_map<int, int> &document_term_count);

// Varbyte encode function
std::vector<uint8_t> varbyteEncode(int number, int &size);
// Varbyte decode function
int varbyteDecode(const std::vector<uint8_t> &encoded);

// write to file
void writeIndexToFile(const std::unordered_map<int, std::vector<std::pair<int, int>>> &index,
                      const std::unordered_map<int, std::string> &term_id_to_word,
                      int file_number);

// write document info to file
void writeDocumentInfoToFile(const std::unordered_map<int, int> &document_term_count);

// external sort
void externalSort(int num_files, const std::unordered_map<std::string, LexiconInfo> &lexicon,
                  const std::unordered_map<int, std::string> &term_id_to_word);
// write merged postings
void writeMergedPostings(std::ofstream &outfile, const std::string &word,
                         const std::vector<std::pair<int, int>> &postings,
                         const std::unordered_map<std::string, LexiconInfo> &lexicon);

// Posting struct
struct Posting
{
    int doc_id;
    int total_term;
};

// LexiconInfo struct
struct LexiconInfo
{
    int term_id;
    int start_doc_id;
    int end_doc_id;
    int length;
};

struct IndexEntry
{
    int term_id;
    int file_index;
    std::vector<std::pair<int, int>> postings;

    IndexEntry(int t, int fi, const std::vector<std::pair<int, int>> &p)
        : term_id(t), file_index(fi), postings(p) {}
};

struct CompareIndexEntry
{
    const std::unordered_map<int, std::string> *term_id_to_word;

    CompareIndexEntry() : term_id_to_word(nullptr) {}
    CompareIndexEntry(const std::unordered_map<int, std::string> *map) : term_id_to_word(map) {}

    bool operator()(const IndexEntry &a, const IndexEntry &b) const
    {
        return term_id_to_word->at(a.term_id) > term_id_to_word->at(b.term_id);
    }
};

// Process sentence part
std::vector<std::string> processSentencePart(const std::string &sentence_part)
{
    std::vector<std::string> words;
    std::string current_word;
    current_word.reserve(50); // preallocate memory for current word, let's say 50 characters

    for (char c : sentence_part)
    {
        if (std::isalpha(c))
        {
            current_word += std::tolower(c);
        }
        else if (std::isdigit(c))
        {
            current_word += c;
        }
        else if (!current_word.empty())
        {
            words.push_back(current_word);
            current_word.clear();
        }
    }

    if (!current_word.empty())
    {
        words.push_back(current_word);
        current_word.clear();
    }

    return words;
}

// Process line
size_t processLine(const std::string &line,
                   std::unordered_map<int, int> &document_term_count,
                   std::unordered_map<int, std::vector<std::pair<int, int>>> &index,
                   std::unordered_map<std::string, LexiconInfo> &lexicon,
                   std::unordered_map<int, std::string> &term_id_to_word,
                   int &last_doc_id, int &term_id)
{
    std::istringstream iss(line);
    int doc_id;
    if (!(iss >> doc_id) || doc_id < last_doc_id)
    {
        std::cerr << "Invalid doc_id: " << doc_id << ", last_doc_id: " << last_doc_id << std::endl;
        return 0;
    }
    std::string sentence_part;
    std::unordered_map<std::string, int> word_counts;
    size_t memory_increment = 0;
    memory_increment += sizeof(int); // for document info

    while (iss >> sentence_part)
    {
        std::vector<std::string> words = processSentencePart(sentence_part);
        for (const std::string &word : words)
        {
            if (!word.empty())
            {
                word_counts[word]++;
                document_term_count[doc_id]++;
            }
        }
    }

    for (const auto &[word, count] : word_counts)
    {
        if (lexicon.find(word) == lexicon.end())
        {
            lexicon[word] = LexiconInfo{term_id, doc_id, doc_id, 1};
            term_id_to_word[term_id] = word;
            memory_increment += word.capacity() + sizeof(LexiconInfo);
            term_id++;
        }

        auto &info = lexicon[word];
        int diff = doc_id - info.end_doc_id;
        info.end_doc_id = doc_id;
        info.length++;
        index[info.term_id].push_back({diff, count});

        memory_increment += sizeof(std::pair<int, int>);
        if (index[info.term_id].size() == 1)
        {
            memory_increment += sizeof(int) + sizeof(std::vector<std::pair<int, int>>);
        }
    }
    std::cout << "Processed line: " << doc_id << ", memory increment: " << memory_increment
              << ", words: " << word_counts.size() << std::endl;
    last_doc_id = doc_id;

    return memory_increment;
}

// Process tar.gz file
void processTarGz(const std::string &filename, int chunk_size)
{
    struct archive *a;
    struct archive_entry *entry;
    int r;

    // initialize archive
    a = archive_read_new();
    archive_read_support_filter_gzip(a);
    archive_read_support_format_tar(a);

    r = archive_read_open_filename(a, filename.c_str(), 10240);
    if (r != ARCHIVE_OK)
    {
        std::cerr << "Cannot open file: " << filename << ", error info: " << archive_error_string(a) << std::endl;
        return;
    }

    std::unordered_map<int, std::vector<std::pair<int, int>>> index;
    std::unordered_map<std::string, LexiconInfo> lexicon;
    std::unordered_map<int, int> document_term_count;
    std::unordered_map<int, std::string> term_id_to_word;
    size_t current_memory_usage = 0;
    int file_counter = 0;

    while (archive_read_next_header(a, &entry) == ARCHIVE_OK)
    {
        if (archive_entry_filetype(entry) == AE_IFREG)
        {
            const char *currentFile = archive_entry_pathname(entry);
            size_t size = archive_entry_size(entry);
            if (size == 0)
                continue;

            std::unique_ptr<char[]> buffer(new char[chunk_size]);
            size_t total_bytes_read = 0;
            int last_doc_id = -1;
            int term_id = 0;
            std::string leftover; // for storing the remaining part of the last block

            while (total_bytes_read < size)
            {
                size_t bytesRead = archive_read_data(a, buffer.get(), chunk_size);
                if (bytesRead < 0)
                {
                    std::cerr << "Error reading data from archive: " << archive_error_string(a) << std::endl;
                    break;
                }
                total_bytes_read += bytesRead;

                std::string chunk(buffer.get(), bytesRead);
                std::istringstream content(leftover + chunk);
                std::string line;
                leftover.clear();

                while (std::getline(content, line))
                {
                    if (content.eof() && line.back() != '\n')
                    {
                        // if the last line is not complete, save it to leftover
                        leftover = line;
                        break;
                    }

                    size_t memory_increment = processLine(line, document_term_count, index, lexicon, term_id_to_word, last_doc_id, term_id);
                    current_memory_usage += memory_increment;

                    if (current_memory_usage > MEMORY_LIMIT)
                    {
                        writeIndexToFile(index, term_id_to_word, file_counter++);
                        index.clear();
                        current_memory_usage = estimateMemoryUsage(index, lexicon, term_id_to_word, document_term_count);
                    }
                }
            }

            // process the last incomplete line
            if (!leftover.empty())
            {
                size_t memory_increment = processLine(leftover, document_term_count, index, lexicon, term_id_to_word, last_doc_id, term_id);
                current_memory_usage += memory_increment;
            }

            buffer.reset();
        }
    }

    // process remaining data in index
    if (!index.empty())
    {
        writeIndexToFile(index, term_id_to_word, file_counter++);
    }

    // write document info to file after processing all lines
    writeDocumentInfoToFile(document_term_count);
    document_term_count.clear();
    index.clear();
    current_memory_usage = estimateMemoryUsage(index, lexicon, term_id_to_word, document_term_count);

    archive_read_close(a);
    archive_read_free(a);

    // external sort
    externalSort(file_counter, lexicon, term_id_to_word);
}

// Estimate memory usage
size_t estimateMemoryUsage(const std::unordered_map<int, std::vector<std::pair<int, int>>> &index,
                           const std::unordered_map<std::string, LexiconInfo> &lexicon,
                           const std::unordered_map<int, std::string> &term_id_to_word,
                           const std::unordered_map<int, int> &document_term_count)
{
    size_t usage = 0;
    for (const auto &[term_id, postings] : index)
    {
        usage += sizeof(int) + sizeof(std::vector<std::pair<int, int>>) + postings.capacity() * sizeof(std::pair<int, int>);
    }
    for (const auto &[word, info] : lexicon)
    {
        usage += word.capacity() + sizeof(LexiconInfo);
    }
    for (const auto &[doc_id, count] : document_term_count)
    {
        usage += sizeof(int);
    }
    for (const auto &[term_id, word] : term_id_to_word)
    {
        usage += word.capacity() + sizeof(std::string);
    }
    return usage;
}

// Varbyte encode function
std::vector<uint8_t> varbyteEncode(int number, int &size)
{
    std::vector<uint8_t> bytes;
    while (number >= 128)
    {
        bytes.push_back((number & 0x7F) | 0x80);
        number >>= 7;
    }
    bytes.push_back(number & 0x7F);
    size = bytes.size();
    return bytes;
}

// Varbyte decode function
int varbyteDecode(const std::vector<uint8_t> &encoded)
{
    int number = 0;
    int shift = 0;

    for (size_t i = 0; i < encoded.size(); ++i)
    {
        number |= (encoded[i] & 0x7F) << shift;
        if (!(encoded[i] & 0x80))
            break;
        shift += 7;
    }

    return number;
}

// Write index to file
void writeIndexToFile(const std::unordered_map<int, std::vector<std::pair<int, int>>> &index,
                      const std::unordered_map<int, std::string> &term_id_to_word,
                      int file_number)
{
    std::string filename = "temp_index_" + std::to_string(file_number) + ".txt";
    std::ofstream outfile(filename);

    // create a vector of term_id
    std::vector<int> sorted_term_ids;
    for (const auto &[term_id, _] : index)
    {
        sorted_term_ids.push_back(term_id);
    }

    // sort term_id by the order of the corresponding words
    std::sort(sorted_term_ids.begin(), sorted_term_ids.end(),
              [&term_id_to_word](int a, int b)
              {
                  return term_id_to_word.at(a) < term_id_to_word.at(b);
              });

    // write sorted index
    for (const int term_id : sorted_term_ids)
    {
        outfile << term_id << " ";
        for (const auto &[diff, count] : index.at(term_id))
        {
            outfile << diff << " " << count << " ";
        }
        outfile << "\n";
    }
    outfile.close();
}

// Write document info to file
void writeDocumentInfoToFile(const std::unordered_map<int, int> &document_term_count)
{
    std::ofstream outfile("document_term_count.txt");
    for (const auto &[doc_id, count] : document_term_count)
    {
        outfile << doc_id << " " << count << "\n";
    }
    outfile.close();
}

// External sort
void externalSort(int num_files, const std::unordered_map<std::string, LexiconInfo> &lexicon,
                  const std::unordered_map<int, std::string> &term_id_to_word)
{
    CompareIndexEntry comparator(&term_id_to_word);
    std::priority_queue<IndexEntry, std::vector<IndexEntry>, CompareIndexEntry> pq(comparator);
    std::vector<std::ifstream> files(num_files);

    for (int i = 0; i < num_files; ++i)
    {
        files[i].open("temp_index_" + std::to_string(i) + ".txt");
        std::string line;
        if (std::getline(files[i], line))
        {
            std::istringstream iss(line);
            int term_id;
            iss >> term_id;
            std::vector<std::pair<int, int>> postings;
            int diff, count;
            while (iss >> diff >> count)
            {
                postings.push_back({diff, count});
            }
            pq.push(IndexEntry(term_id, i, postings));
        }
    }

    std::ofstream outfile("final_sorted_index.txt");
    std::vector<std::pair<int, int>> merged_postings;
    int current_term_id = -1;
    size_t memory_usage = 0;

    while (!pq.empty())
    {
        auto top = pq.top();
        pq.pop();

        if (current_term_id != top.term_id)
        {
            if (current_term_id != -1)
            {
                writeMergedPostings(outfile, term_id_to_word.at(current_term_id), merged_postings, lexicon);
                memory_usage -= merged_postings.capacity() * sizeof(std::pair<int, int>);
                merged_postings.clear();
            }
            current_term_id = top.term_id;
        }

        merged_postings.insert(merged_postings.end(), top.postings.begin(), top.postings.end());
        memory_usage += top.postings.capacity() * sizeof(std::pair<int, int>);

        if (memory_usage > MEMORY_LIMIT)
        {
            writeMergedPostings(outfile, term_id_to_word.at(current_term_id), merged_postings, lexicon);
            memory_usage = 0;
            merged_postings.clear();
            current_term_id = -1;
        }

        std::string line;
        if (std::getline(files[top.file_index], line))
        {
            std::istringstream iss(line);
            int term_id;
            iss >> term_id;
            std::vector<std::pair<int, int>> postings;
            int diff, count;
            while (iss >> diff >> count)
            {
                postings.push_back({diff, count});
            }
            pq.push(IndexEntry(term_id, top.file_index, postings));
        }
    }

    if (!merged_postings.empty())
    {
        writeMergedPostings(outfile, term_id_to_word.at(current_term_id), merged_postings, lexicon);
    }

    outfile.close();
    for (auto &file : files)
    {
        file.close();
    }

    // delete temp files
    for (int i = 0; i < num_files; ++i)
    {
        std::remove(("temp_index_" + std::to_string(i) + ".txt").c_str());
    }
}

void writeMergedPostings(std::ofstream &outfile, const std::string &word,
                         const std::vector<std::pair<int, int>> &postings,
                         const std::unordered_map<std::string, LexiconInfo> &lexicon)
{
    outfile << word << " ";
    const auto &info = lexicon.at(word);
    outfile << info.start_doc_id << " " << info.end_doc_id << " " << info.length << " ";
    for (const auto &[diff, count] : postings)
    {
        outfile << diff << " " << count << " ";
    }
    outfile << "\n";
}

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        std::cerr << "Usage: " << argv[0] << " <gz file path>" << std::endl;
        return 1;
    }

    std::string filename = argv[1];
    processTarGz(filename, CHUNK_SIZE);
    std::cout << "done" << std::endl;
    return 0;
}
