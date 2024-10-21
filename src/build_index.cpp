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
struct IndexEntry;
struct CompareIndexEntry;
struct Posting;
struct LexiconInfo;

// estimate memory usage
size_t estimateIndexMemoryUsage(const std::unordered_map<std::string, std::vector<std::pair<int, int>>> &index);
size_t estimateMemoryUsage(const std::unordered_map<std::string, std::vector<std::pair<int, int>>> &index,
                           const std::unordered_map<std::string, LexiconInfo> &lexicon);
// write to file
void writeToFile(const std::unordered_map<std::string, std::vector<std::pair<int, int>>> &index,
                 int file_number);
// external sort
void externalSort(int num_files, const std::unordered_map<std::string, LexiconInfo> &lexicon);
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
    int start_doc_id;
    int end_doc_id;
    int length;
};

struct IndexEntry
{
    std::string word;
    int file_index;
    std::vector<std::pair<int, int>> postings;

    IndexEntry(const std::string &w, int fi, const std::vector<std::pair<int, int>> &p)
        : word(w), file_index(fi), postings(p) {}
};

struct CompareIndexEntry
{
    bool operator()(const IndexEntry &a, const IndexEntry &b)
    {
        return a.word > b.word;
    }
};

// process sentence part
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

// process line
size_t processLine(const std::string &line,
                   std::unordered_map<std::string, std::vector<std::pair<int, int>>> &index,
                   std::unordered_map<std::string, LexiconInfo> &lexicon, int &last_doc_id)
{
    std::istringstream iss(line);
    int doc_id;
    if (!(iss >> doc_id) || doc_id < last_doc_id)
    {
        return 0;
    }
    std::string sentence_part;
    std::unordered_map<std::string, int> word_counts;
    size_t memory_increment = 0;

    while (iss >> sentence_part)
    {
        std::vector<std::string> words = processSentencePart(sentence_part);
        for (const std::string &word : words)
        {
            if (!word.empty())
                word_counts[word]++;
        }
    }

    for (const auto &[word, count] : word_counts)
    {
        if (lexicon.find(word) == lexicon.end())
        {
            lexicon[word] = LexiconInfo{doc_id, doc_id, 1};
            memory_increment += word.capacity() + sizeof(LexiconInfo);
        }

        auto &info = lexicon[word];
        int diff = doc_id - info.end_doc_id;
        if (diff < 0)
        {
            std::cout << "doc_id: " << doc_id << ", end_doc_id: " << info.end_doc_id << ", diff: " << diff << std::endl;
            std::cout << "line: " << line << std::endl;

            exit(EXIT_FAILURE); // terminate the program
        }
        info.end_doc_id = doc_id;
        info.length++;
        index[word].push_back({diff, count});

        memory_increment += sizeof(std::pair<int, int>);
        if (index[word].size() == 1)
        {
            memory_increment += word.capacity() + sizeof(std::vector<std::pair<int, int>>);
        }
    }
    std::cout << "processed line: " << doc_id << ", memory increment: " << memory_increment << std::endl;
    last_doc_id = doc_id;

    return memory_increment;
}

// process tar.gz file
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

    std::unordered_map<std::string, std::vector<std::pair<int, int>>> index;
    std::unordered_map<std::string, LexiconInfo> lexicon;
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

            char *buffer = new char[chunk_size];
            size_t total_bytes_read = 0;
            int last_doc_id = -1;

            while (total_bytes_read < size)
            {
                size_t bytesRead = archive_read_data(a, buffer, chunk_size);
                if (bytesRead < 0)
                {
                    std::cerr << "Error reading data from archive: " << archive_error_string(a) << std::endl;
                    break;
                }
                total_bytes_read += bytesRead;

                std::istringstream content(std::string(buffer, bytesRead));
                std::string line;
                std::string leftover;

                while (std::getline(content, line))
                {
                    if (!leftover.empty())
                    {
                        line = leftover + line;
                        leftover.clear();
                    }
                    if (content.peek() == EOF && line.back() != '\n')
                    {
                        leftover = line;
                        continue;
                    }
                    size_t memory_increment = processLine(line, index, lexicon, last_doc_id);
                    current_memory_usage += memory_increment;

                    if (current_memory_usage > MEMORY_LIMIT)
                    {
                        writeToFile(index, file_counter++);
                        size_t index_memory = estimateIndexMemoryUsage(index);
                        index.clear();
                        current_memory_usage = estimateMemoryUsage(index, lexicon);
                    }
                }
            }
            delete[] buffer;
        }
    }

    // process remaining data
    if (!index.empty())
    {
        writeToFile(index, file_counter++);
    }

    archive_read_close(a);
    archive_read_free(a);

    // external sort
    externalSort(file_counter, lexicon);
}

// estimate memory usage
size_t estimateMemoryUsage(const std::unordered_map<std::string, std::vector<std::pair<int, int>>> &index,
                           const std::unordered_map<std::string, LexiconInfo> &lexicon)
{
    size_t usage = 0;
    for (const auto &[word, postings] : index)
    {
        usage += word.capacity() + sizeof(std::vector<std::pair<int, int>>) + postings.capacity() * sizeof(std::pair<int, int>);
    }
    for (const auto &[word, info] : lexicon)
    {
        usage += word.capacity() + sizeof(LexiconInfo);
    }
    return usage;
}

// estimate index memory usage
size_t estimateIndexMemoryUsage(const std::unordered_map<std::string, std::vector<std::pair<int, int>>> &index)
{
    size_t usage = 0;
    for (const auto &[word, postings] : index)
    {
        usage += word.capacity() + sizeof(std::vector<std::pair<int, int>>) + postings.capacity() * sizeof(std::pair<int, int>);
    }
    return usage;
}

// write to file
void writeToFile(const std::unordered_map<std::string, std::vector<std::pair<int, int>>> &index,
                 int file_number)
{
    std::string filename = "temp_index_" + std::to_string(file_number) + ".txt";
    std::ofstream outfile(filename);

    // create a sorted vector
    std::vector<std::pair<std::string, std::vector<std::pair<int, int>>>> sorted_index(index.begin(), index.end());

    // sort by key alphabetically
    std::sort(sorted_index.begin(), sorted_index.end(),
              [](const auto &a, const auto &b)
              { return a.first < b.first; });

    // write sorted index
    for (const auto &[word, postings] : sorted_index)
    {
        outfile << word << " ";
        for (const auto &[diff, count] : postings)
        {
            outfile << diff << " " << count << " ";
        }
        outfile << "\n";
    }
    outfile.close();
}

// external sort
void externalSort(int num_files, const std::unordered_map<std::string, LexiconInfo> &lexicon)
{
    std::priority_queue<IndexEntry, std::vector<IndexEntry>, CompareIndexEntry> pq;
    std::vector<std::ifstream> files(num_files);

    for (int i = 0; i < num_files; ++i)
    {
        files[i].open("temp_index_" + std::to_string(i) + ".txt");
        std::string line;
        if (std::getline(files[i], line))
        {
            std::istringstream iss(line);
            std::string word;
            iss >> word;
            std::vector<std::pair<int, int>> postings;
            int diff, count;
            while (iss >> diff >> count)
            {
                postings.push_back({diff, count});
            }
            pq.push(IndexEntry(word, i, postings));
        }
    }

    std::ofstream outfile("final_sorted_index.txt");
    std::vector<std::pair<int, int>> merged_postings;
    std::string current_word;
    size_t memory_usage = 0;

    while (!pq.empty())
    {
        auto top = pq.top();
        pq.pop();

        if (current_word != top.word)
        {
            if (!current_word.empty())
            {
                writeMergedPostings(outfile, current_word, merged_postings, lexicon);
                memory_usage -= (current_word.capacity() + merged_postings.capacity() * sizeof(std::pair<int, int>));
                merged_postings.clear();
            }
            current_word = top.word;
        }

        merged_postings.insert(merged_postings.end(), top.postings.begin(), top.postings.end());
        memory_usage += top.postings.capacity() * sizeof(std::pair<int, int>);

        if (memory_usage > MEMORY_LIMIT)
        {
            writeMergedPostings(outfile, current_word, merged_postings, lexicon);
            memory_usage = 0;
            merged_postings.clear();
            current_word.clear();
        }

        std::string line;
        if (std::getline(files[top.file_index], line))
        {
            std::istringstream iss(line);
            std::string word;
            iss >> word;
            std::vector<std::pair<int, int>> postings;
            int diff, count;
            while (iss >> diff >> count)
            {
                postings.push_back({diff, count});
            }
            pq.push(IndexEntry(word, top.file_index, postings));
        }
    }

    if (!merged_postings.empty())
    {
        writeMergedPostings(outfile, current_word, merged_postings, lexicon);
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
