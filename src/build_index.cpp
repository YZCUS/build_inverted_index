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

const int CHUNK_SIZE = 1024 * 64;             // 64KB
const std::string TEMP_DIR = "temp_index";    // temp directory
const size_t MEMORY_LIMIT = 50 * 1024 * 1024; // 50MB, leave space for lexicon and other operations
const int SMALL_DOC_TEST = 400000;

// forward declarations
struct Posting;
struct LexiconInfo;
struct IndexEntry;
struct CompareIndexEntry;

// estimate memory usage
size_t estimateMemoryUsage(const std::unordered_map<int, std::vector<std::pair<int, int>>> &index,
                           const std::unordered_map<std::string, LexiconInfo> &lexicon,
                           const std::unordered_map<int, std::string> &term_id_to_word,
                           const std::unordered_map<int, std::pair<int, int64_t>> &document_info);

// Varbyte encode function
std::vector<uint8_t> varbyteEncode(uint32_t number);
// Varbyte decode function
uint32_t varbyteDecode(const std::vector<uint8_t> &bytes);

// write to file
void writeIndexToFile(const std::unordered_map<int, std::vector<std::pair<int, int>>> &index,
                      const std::unordered_map<int, std::string> &term_id_to_word,
                      int file_number);

// write document info to file
void writeDocumentInfoToFile(const std::unordered_map<int, std::pair<int, int64_t>> &document_info);

// external sort
void externalSort(int num_files, std::unordered_map<std::string, LexiconInfo> &lexicon,
                  const std::unordered_map<int, std::string> &term_id_to_word);

// read next entry
IndexEntry readNextEntry(std::ifstream &file, int file_index, const std::unordered_map<int, std::string> &term_id_to_word);

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
    int end_doc_id;
    int posting_number;
    int64_t start_position;
    int64_t bytes_size;
};

// IndexEntry struct
struct IndexEntry
{
    int term_id;
    int file_index; // add file_index member
    std::streamoff file_position;
    std::vector<std::pair<int, int>> postings;

    IndexEntry(int t, int fi, std::streamoff fp, std::vector<std::pair<int, int>> &&p)
        : term_id(t), file_index(fi), file_position(fp), postings(std::move(p)) {}
};

// CompareIndexEntry struct
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
                   std::unordered_map<int, std::pair<int, int64_t>> &document_info,
                   std::unordered_map<int, std::vector<std::pair<int, int>>> &index,
                   std::unordered_map<std::string, LexiconInfo> &lexicon,
                   std::unordered_map<int, std::string> &term_id_to_word,
                   int &last_doc_id, int &term_id,
                   std::streamoff &line_position)
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

    // update document info of position of doc_id
    document_info[doc_id] = {0, line_position + iss.tellg()};

    while (iss >> sentence_part)
    {
        std::vector<std::string> words = processSentencePart(sentence_part);
        for (const std::string &word : words)
        {
            if (!word.empty())
            {
                word_counts[word]++;
                document_info[doc_id].first++;
            }
        }
    }

    if (line_position < 0)
    {
        std::cerr << "line_position is negative: " << line_position << std::endl;
        std::cerr << "offset: " << iss.tellg() << std::endl;
        exit(1);
    }

    for (const auto &[word, count] : word_counts)
    {
        if (lexicon.find(word) == lexicon.end())
        {
            lexicon[word] = LexiconInfo{term_id, 0, 0, 0, 0};
            term_id_to_word[term_id] = word;
            memory_increment += word.capacity() + sizeof(LexiconInfo);
            term_id++;
        }

        auto &info = lexicon[word];
        int diff = doc_id - info.end_doc_id;
        info.end_doc_id = doc_id;
        info.posting_number++;
        index[info.term_id].push_back({diff, count});

        memory_increment += sizeof(std::pair<int, int>);
        if (index[info.term_id].size() == 1)
        {
            memory_increment += sizeof(int) + sizeof(std::vector<std::pair<int, int>>);
        }
    }
    if (doc_id % 100000 == 0)
    {
        std::cout << "Processed line: " << doc_id << ", memory increment: " << memory_increment
                  << ", words: " << word_counts.size() << std::endl;
    }
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
    std::unordered_map<int, std::pair<int, int64_t>> document_info;
    std::unordered_map<int, std::string> term_id_to_word;
    size_t current_memory_usage = 0;
    int file_counter = 0;
    int last_doc_id = 0;
    int term_id = 0;
    std::streamoff line_position = 0;

    while (archive_read_next_header(a, &entry) == ARCHIVE_OK)
    {
        if (archive_entry_filetype(entry) == AE_IFREG && last_doc_id < SMALL_DOC_TEST)
        {
            const char *currentFile = archive_entry_pathname(entry);
            size_t size = archive_entry_size(entry);
            if (size == 0)
                continue;

            std::unique_ptr<char[]> buffer(new char[chunk_size]);
            size_t total_bytes_read = 0;
            std::string leftover; // for storing the remaining part of the last block

            while (total_bytes_read < size && last_doc_id < SMALL_DOC_TEST)
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

                while (std::getline(content, line) && last_doc_id < SMALL_DOC_TEST)
                {
                    if (content.eof() && line.back() != '\n')
                    {
                        // if the last line is not complete, save it to leftover
                        leftover = line;
                        break;
                    }

                    size_t memory_increment = processLine(line, document_info, index, lexicon, term_id_to_word, last_doc_id, term_id, line_position);
                    line_position += line.size() + 1; // +1 for '\n'
                    current_memory_usage += memory_increment;

                    if (current_memory_usage > MEMORY_LIMIT || last_doc_id >= SMALL_DOC_TEST)
                    {
                        writeIndexToFile(index, term_id_to_word, file_counter++);
                        index.clear();
                        current_memory_usage = estimateMemoryUsage(index, lexicon, term_id_to_word, document_info);
                    }
                }
            }

            // process the last incomplete line
            if (!leftover.empty() && last_doc_id < SMALL_DOC_TEST)
            {
                size_t memory_increment = processLine(leftover, document_info, index, lexicon, term_id_to_word, last_doc_id, term_id, line_position);
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
    writeDocumentInfoToFile(document_info);
    std::cout << "document_info size: " << document_info.size() << std::endl;
    document_info.clear();
    index.clear();
    current_memory_usage = estimateMemoryUsage(index, lexicon, term_id_to_word, document_info);

    archive_read_close(a);
    archive_read_free(a);

    // external sort
    std::cout << "total_term: " << term_id_to_word.size() << std::endl;
    externalSort(file_counter, lexicon, term_id_to_word);
}

// Estimate memory usage
size_t estimateMemoryUsage(const std::unordered_map<int, std::vector<std::pair<int, int>>> &index,
                           const std::unordered_map<std::string, LexiconInfo> &lexicon,
                           const std::unordered_map<int, std::string> &term_id_to_word,
                           const std::unordered_map<int, std::pair<int, int64_t>> &document_info)
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
    for (const auto &[doc_id, pair] : document_info)
    {
        usage += sizeof(int) + sizeof(std::pair<int, int64_t>);
    }
    for (const auto &[term_id, word] : term_id_to_word)
    {
        usage += word.capacity() + sizeof(std::string);
    }
    return usage;
}

// Varbyte encode function
std::vector<uint8_t> varbyteEncode(uint32_t number)
{
    std::vector<uint8_t> bytes;
    while (number >= 128)
    {
        bytes.push_back((number & 127) | 128);
        number >>= 7;
    }
    bytes.push_back(number);
    return bytes;
}

// Varbyte decode function
uint32_t varbyteDecode(const std::vector<uint8_t> &bytes)
{
    uint32_t number = 0;
    for (int i = bytes.size() - 1; i >= 0; --i)
    {
        number = (number << 7) | (bytes[i] & 127);
    }
    return number;
}

// Write index to file
void writeIndexToFile(const std::unordered_map<int, std::vector<std::pair<int, int>>> &index,
                      const std::unordered_map<int, std::string> &term_id_to_word,
                      int file_number)
{
    std::string filename = "temp_index_" + std::to_string(file_number) + ".bin";
    std::ofstream outfile(filename, std::ios::binary);

    std::vector<int> sorted_term_ids;
    for (const auto &[term_id, _] : index)
    {
        sorted_term_ids.push_back(term_id);
    }

    std::sort(sorted_term_ids.begin(), sorted_term_ids.end(),
              [&term_id_to_word](int a, int b)
              {
                  return term_id_to_word.at(a) < term_id_to_word.at(b);
              });

    for (const int term_id : sorted_term_ids)
    {
        // encode term_id
        auto encoded_term_id = varbyteEncode(term_id);
        outfile.write(reinterpret_cast<const char *>(encoded_term_id.data()), encoded_term_id.size());

        // encode postings count
        const auto &postings = index.at(term_id);
        auto encoded_size = varbyteEncode(postings.size());
        outfile.write(reinterpret_cast<const char *>(encoded_size.data()), encoded_size.size());

        // encode postings
        for (const auto &[diff, count] : postings)
        {
            auto encoded_diff = varbyteEncode(diff);
            auto encoded_count = varbyteEncode(count);
            outfile.write(reinterpret_cast<const char *>(encoded_diff.data()), encoded_diff.size());
            outfile.write(reinterpret_cast<const char *>(encoded_count.data()), encoded_count.size());
        }
    }
    sorted_term_ids.clear();
    outfile.close();
}

// Write document info to file
void writeDocumentInfoToFile(const std::unordered_map<int, std::pair<int, int64_t>> &document_info)
{
    std::ofstream outfile("document_info.txt");
    int max_doc_id = document_info.size() - 1;
    for (int doc_id = 0; doc_id <= max_doc_id; ++doc_id)
    {
        auto pair = document_info.at(doc_id);
        outfile << pair.first << " " << pair.second << "\n";
    }
    outfile.close();
}

// External sort
void externalSort(int num_files,
                  std::unordered_map<std::string, LexiconInfo> &lexicon,
                  const std::unordered_map<int, std::string> &term_id_to_word)
{
    CompareIndexEntry comparator(&term_id_to_word);
    std::priority_queue<IndexEntry, std::vector<IndexEntry>, CompareIndexEntry> pq(comparator);
    std::vector<std::ifstream> files(num_files);

    for (int i = 0; i < num_files; ++i)
    {
        files[i].open("temp_index_" + std::to_string(i) + ".bin", std::ios::binary);
        if (files[i].is_open())
        {
            IndexEntry entry = readNextEntry(files[i], i, term_id_to_word);
            if (entry.term_id != -1)
            {
                pq.push(std::move(entry));
            }
        }
    }

    std::ofstream final_index_file("final_sorted_index.bin", std::ios::binary);
    std::ofstream final_index_file2("final_sorted_index2.txt"); // for debug
    std::ofstream final_lexicon_file("final_sorted_lexicon.txt");
    std::ofstream final_block_info("final_sorted_block_info.bin", std::ios::binary);
    std::ofstream final_block_info2("final_sorted_block_info2.txt"); // for debug

    int64_t current_position = 0;
    int current_term_id = -1;
    int last_doc_id = 0;

    const int POSTING_PER_BLOCK = 128;
    std::vector<std::pair<int, int64_t>> block_info; // store last_doc_id and block size(bytes)
    std::vector<uint8_t> merged_doc_ids;
    std::vector<uint8_t> merged_counts;
    int postings_in_block = 0; // track number of postings in the current block

    while (!pq.empty())
    {
        auto top = pq.top();
        pq.pop();

        if (current_term_id != top.term_id) // new term
        {
            if (current_term_id != -1) // not the first term
            {
                lexicon[term_id_to_word.at(current_term_id)].bytes_size =
                    current_position - lexicon[term_id_to_word.at(current_term_id)].start_position;

                final_lexicon_file << term_id_to_word.at(current_term_id) << " "
                                   << current_term_id << " "
                                   << lexicon[term_id_to_word.at(current_term_id)].posting_number << " "
                                   << lexicon[term_id_to_word.at(current_term_id)].start_position << " "
                                   << lexicon[term_id_to_word.at(current_term_id)].bytes_size << "\n";
            }

            lexicon[term_id_to_word.at(top.term_id)].start_position = current_position;
            current_term_id = top.term_id;
            last_doc_id = 0;
        }
        final_index_file2 << top.term_id << " " << top.postings.size() << " ";
        for (const auto &[diff, count] : top.postings)
        {
            final_index_file2 << diff << " " << count << " ";
            auto encoded_diff = varbyteEncode(diff);
            auto encoded_count = varbyteEncode(count);

            // add encoded_diff and encoded_count to buffers
            merged_doc_ids.insert(merged_doc_ids.end(), encoded_diff.begin(), encoded_diff.end());
            merged_counts.insert(merged_counts.end(), encoded_count.begin(), encoded_count.end());

            // update current position
            current_position += encoded_diff.size() + encoded_count.size();
            last_doc_id += diff;
            postings_in_block++; // increment postings count

            // check if need to write new block
            if (postings_in_block == POSTING_PER_BLOCK)
            {
                // add buffer to final_index_file
                final_index_file.write(reinterpret_cast<const char *>(merged_doc_ids.data()), merged_doc_ids.size()); // writing 128 doc_ids
                final_index_file.write(reinterpret_cast<const char *>(merged_counts.data()), merged_counts.size());   // writing 128 counts
                int current_block_size = merged_doc_ids.size() + merged_counts.size();
                block_info.emplace_back(last_doc_id, current_block_size); // store the last doc_id and the block size
                final_block_info2 << last_doc_id << " " << current_block_size << "\n";

                // clear buffer and reset postings count
                merged_doc_ids.clear();
                merged_counts.clear();
                postings_in_block = 0;
            }
        }
        final_index_file2 << "\n";

        // when need to reposition the file pointer
        files[top.file_index].seekg(top.file_position);
        IndexEntry entry = readNextEntry(files[top.file_index], top.file_index, term_id_to_word);
        if (entry.term_id != -1)
        {
            pq.push(std::move(entry));
        }
    }

    // process the last block
    if (postings_in_block > 0)
    {
        final_index_file.write(reinterpret_cast<const char *>(merged_doc_ids.data()), merged_doc_ids.size());
        final_index_file.write(reinterpret_cast<const char *>(merged_counts.data()), merged_counts.size());
        int current_block_size = merged_doc_ids.size() + merged_counts.size();
        block_info.emplace_back(last_doc_id, current_block_size);
        final_block_info2 << last_doc_id << " " << current_block_size << "\n";
    }

    // write the last block info into the file
    final_block_info.write(reinterpret_cast<const char *>(block_info.data()), block_info.size() * sizeof(std::pair<int, int64_t>));

    final_index_file.close();
    final_lexicon_file.close();
    final_block_info.close();
    final_index_file2.close();
    final_block_info2.close();
    for (auto &file : files)
    {
        file.close();
    }

    // delete temp files
    for (int i = 0; i < num_files; ++i)
    {
        std::remove(("temp_index_" + std::to_string(i) + ".bin").c_str());
    }
}

// read next entry
IndexEntry readNextEntry(std::ifstream &file, int file_index, const std::unordered_map<int, std::string> &term_id_to_word)
{
    std::vector<uint8_t> buffer;
    uint8_t byte;

    // read term_id
    while (file.read(reinterpret_cast<char *>(&byte), 1))
    {
        buffer.push_back(byte);
        if (!(byte & 0x80))
            break;
    }
    if (buffer.empty())
        return {-1, file_index, 0, {}}; // file end
    int term_id = varbyteDecode(buffer);
    buffer.clear();

    // read postings count
    while (file.read(reinterpret_cast<char *>(&byte), 1))
    {
        buffer.push_back(byte);
        if (!(byte & 0x80))
            break;
    }
    int postings_count = varbyteDecode(buffer);
    buffer.clear();

    // read postings
    std::vector<std::pair<int, int>> postings;
    for (int i = 0; i < postings_count; ++i)
    {
        // read diff
        while (file.read(reinterpret_cast<char *>(&byte), 1))
        {
            buffer.push_back(byte);
            if (!(byte & 0x80))
                break;
        }
        int diff = varbyteDecode(buffer);
        buffer.clear();

        // read count
        while (file.read(reinterpret_cast<char *>(&byte), 1))
        {
            buffer.push_back(byte);
            if (!(byte & 0x80))
                break;
        }
        int count = varbyteDecode(buffer);
        buffer.clear();

        postings.emplace_back(diff, count);
    }

    return {term_id, file_index, file.tellg(), std::move(postings)};
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
