#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <queue>
#include <cmath>
#include <sstream>
#include <cstdint>
#include <zlib.h>

const int POSTING_PER_BLOCK = 128;
const std::string LEXICON_FILE = "final_sorted_lexicon.txt";
const std::string INDEX_FILE = "final_sorted_index.bin";
const std::string DOC_INFO_FILE = "document_info.txt";
const std::string BLOCK_INFO_FILE = "final_sorted_block_info2.txt";
const std::string ORIGINAL_TAR_GZ = "../src/collection.tar.gz";

// parameters
const double k1 = 1.2;
const double b = 0.75;

// decode function
uint32_t varbyteDecode(const std::vector<uint8_t> &bytes);
int varbyteDecode(const uint8_t *data, size_t max_size, size_t &bytes_read);
size_t varbyteEncodedSize(int value);

struct LexiconEntry
{
    int term_id;
    int postings_num;
    int64_t start_position;
    int64_t bytes_size;
};

struct SearchResult
{
    int doc_id;
    double score;
};

class InvertedList
{
private:
    std::ifstream &index_file_;
    int64_t start_pos_;
    int64_t bytes_size_;
    size_t current_pos_;
    std::vector<std::pair<int, int>> &block_info_;
    std::vector<uint8_t> current_block_;
    int current_block_index_;

    void loadBlockIndex()
    {
        std::cout << "Loading block index." << std::endl;
        for (int i = 0; i < block_info_.size(); ++i)
        {
            if (start_pos_ < block_info_[i].second)
            {
                current_block_index_ = i - 1;
                break;
            }
        }
        std::cout << "Block index loaded. Current block index: " << current_block_index_ << std::endl;
    }

    void openBlock()
    {
        std::cout << "Opening block." << std::endl;
        index_file_.seekg(block_info_[current_block_index_].second);
        if (current_block_index_ == block_info_.size() - 1) // last block
        {
            bytes_size_ = bytes_size_ - block_info_[current_block_index_].second;
        }
        else // not the last block
        {
            bytes_size_ = block_info_[current_block_index_ + 1].second - block_info_[current_block_index_].second;
        }
        current_block_.resize(bytes_size_);
        index_file_.read(reinterpret_cast<char *>(current_block_.data()), bytes_size_); // read the block into memory
        current_pos_ = 0;                                                               // reset the current position
    }

    bool loadNextBlock()
    {
        std::cout << "Loading next block." << std::endl;
        current_block_index_++;
        if (current_block_index_ == block_info_.size()) // no more blocks
        {
            return false;
        }
        openBlock();
        return true;
    }

public:
    InvertedList(std::ifstream &index_file, int64_t start_pos, int64_t bytes_size, std::vector<std::pair<int, int>> &block_info)
        : index_file_(index_file), start_pos_(start_pos), bytes_size_(bytes_size), block_info_(block_info)
    {
        std::cout << "Inverted list initialized. Start pos: " << start_pos_ << ", Size: " << bytes_size_ << " bytes." << std::endl;
        loadBlockIndex();
        openBlock();
    }

    bool next(int &doc_id, int &freq)
    {
        if (current_pos_ >= POSTING_PER_BLOCK) // Check if we have processed all postings in the current block
        {
            if (!loadNextBlock())
            {
                return false;
            }
            current_pos_ = 0; // Reset position after loading a new block
        }

        size_t bytes_read = 0;

        // Decode the next doc_id diff
        int doc_id_diff = varbyteDecode(current_block_.data() + current_pos_, current_block_.size() - current_pos_, bytes_read);
        current_pos_ += bytes_read; // Move position by the size of the encoded doc_id

        // Decode the corresponding frequency
        int freq_pos = POSTING_PER_BLOCK + current_pos_; // Calculate position for frequency
        freq = varbyteDecode(current_block_.data() + freq_pos, current_block_.size() - freq_pos, bytes_read);

        // Update the doc_id with the decoded difference
        doc_id += doc_id_diff;

        return true;
    }

    int64_t getSize() const { return bytes_size_; }
};

class SearchEngine
{
private: // private members
    std::unordered_map<std::string, LexiconEntry> lexicon;
    std::vector<std::pair<int, int>> block;
    std::unordered_map<int, std::string> term_id_to_word;
    std::ifstream index_file;
    std::ifstream doc_info_file;
    std::ifstream original_file;
    std::vector<int64_t> lines_pos;
    std::vector<int> doc_lengths;
    int total_docs;
    double avg_doc_length;

public: // public members
    SearchEngine(const std::string &lexicon_file, const std::string &index_file,
                 const std::string &doc_info_file, const std::string &block_info_file, const std::string &original_tar_gz)
        : index_file(index_file, std::ios::binary), original_file(original_tar_gz, std::ios::binary)
    {
        loadLexicon(lexicon_file);
        loadBlockInfo(block_info_file);
        loadDocInfo(doc_info_file);
    }

    void loadLexicon(const std::string &lexicon_file)
    {
        std::ifstream lex_file(lexicon_file);
        std::unordered_map<int, std::string> term_id_to_word;
        std::string term;
        LexiconEntry entry;
        std::cout << "Loading lexicon..." << std::endl;
        while (lex_file >> term >> entry.term_id >> entry.postings_num >> entry.start_position >> entry.bytes_size) // tested
        {
            lexicon[term] = entry;
            term_id_to_word[entry.term_id] = term;
        }
        std::cout << "Lexicon loaded." << std::endl;
    }

    void loadBlockInfo(const std::string &block_info_file)
    {
        std::cout << "Loading block info..." << std::endl;
        std::ifstream block_info(block_info_file);
        int last_doc_id = 0;
        int block_start_pos = 0;
        int block_size = 0;
        while (block_info >> last_doc_id >> block_size) // tested
        {
            block.push_back({last_doc_id, block_start_pos});
            block_start_pos += block_size;
        }
        std::cout << "Block info loaded." << std::endl;
    }

    void loadDocInfo(const std::string &doc_info_file)
    {
        std::cout << "Loading doc info..." << std::endl;
        std::ifstream doc_info(doc_info_file);
        int total_length = 0;
        int total_docs = 0;
        int doc_length;
        int64_t line_pos;
        while (doc_info >> doc_length >> line_pos) // tested
        {
            doc_lengths.push_back(doc_length);
            total_length += doc_length;
            ++total_docs;
            lines_pos.push_back(line_pos);
        }
        avg_doc_length = static_cast<double>(total_length) / total_docs;
        std::cout << "Doc info loaded." << std::endl;
    }

    std::string getOriginalFileContent(int doc_id)
    {
        return "document content";
        // Seek to the position in the compressed file
        original_file.seekg(lines_pos[doc_id]);

        // Read the compressed data
        std::string line;
        std::getline(original_file, line);

        return line;
    }

    std::vector<SearchResult> search(const std::string &query, bool conjunctive)
    {
        // process the query
        std::cout << "Processing query..." << std::endl;
        std::vector<std::string> terms = processQuery(query);
        std::cout << "Query processed." << std::endl;
        std::vector<InvertedList> lists;
        // find the inverted lists for the terms
        for (const auto &term : terms)
        {
            std::cout << "Searching for term: " << term << std::endl;
            if (lexicon.find(term) != lexicon.end())
            {
                std::cout << "Found term: " << term << std::endl;
                const auto &entry = lexicon[term];
                lists.emplace_back(index_file, entry.start_position, entry.bytes_size, block);
                std::cout << "Inverted list found for term: " << term << std::endl;
                std::cout << "The term starts at: " << entry.start_position << " with size: " << entry.bytes_size << std::endl;
            }
            else
            {
                std::cout << "Term not found: " << term << std::endl;
            }
        }

        // if no lists are found, return empty vector
        if (lists.empty())
            return {};

        std::vector<SearchResult> results;
        if (conjunctive)
        {
            results = conjunctiveSearch(lists);
        }
        else
        {
            results = disjunctiveSearch(lists);
        }

        std::sort(results.begin(), results.end(),
                  [](const SearchResult &a, const SearchResult &b)
                  { return a.score > b.score; });
        if (results.size() > 10)
            results.resize(10);
        return results;
    }

private: // private methods
    std::vector<std::string> processQuery(const std::string &query)
    {
        std::vector<std::string> terms;
        std::istringstream iss(query);
        std::string term;
        while (iss >> term)
        {
            std::transform(term.begin(), term.end(), term.begin(), ::tolower);
            terms.push_back(term);
        }
        std::cout << "Query terms: ";
        for (const auto &term : terms)
        {
            std::cout << term << " ";
        }
        std::cout << std::endl;
        return terms;
    }

    double computeIDF(int64_t term_freq)
    {
        return std::log((total_docs - term_freq + 0.5) / (term_freq + 0.5) + 1.0);
    }

    double computeTF(int freq, int doc_length)
    {
        return (freq * (k1 + 1)) / (freq + k1 * (1 - b + b * (doc_length / avg_doc_length)));
    }

    std::vector<SearchResult> conjunctiveSearch(std::vector<InvertedList> &lists)
    {
        std::cout << "Conjunctive search..." << std::endl;
        std::vector<SearchResult> results;
        int current_doc = 0;
        std::vector<int> doc_ids(lists.size(), 0);
        std::vector<int> freqs(lists.size(), 0);
        std::cout << "Conjunctive search initialized." << std::endl;

        while (true)
        {
            bool all_equal = true;
            int max_doc = -1;             // Start with an invalid doc_id value
            bool any_list_active = false; // To track if any list is still active

            // Process each inverted list
            for (size_t i = 0; i < lists.size(); ++i)
            {
                // Advance the list until we find a doc_id >= current_doc
                while (doc_ids[i] < current_doc && lists[i].next(doc_ids[i], freqs[i]))
                {
                }
                std::cout << "Doc ID: " << doc_ids[i] << ", Freq: " << freqs[i] << std::endl;

                if (doc_ids[i] != current_doc)
                    all_equal = false;

                // Only consider active lists (i.e., where next() was successful)
                if (lists[i].next(doc_ids[i], freqs[i]))
                {
                    any_list_active = true;

                    // Find the maximum doc_id across all lists
                    if (doc_ids[i] > max_doc)
                        max_doc = doc_ids[i];
                }
            }

            // If no list is active anymore, stop the search
            if (!any_list_active)
                break;

            // If all lists have the same doc_id, calculate the score and move to the next document
            if (all_equal)
            {
                if (current_doc < doc_lengths.size()) // Ensure doc_lengths[current_doc] is valid
                {
                    double score = 0;
                    int doc_length = doc_lengths[current_doc];
                    for (size_t i = 0; i < lists.size(); ++i)
                    {
                        double idf = computeIDF(lists[i].getSize());
                        double tf = computeTF(freqs[i], doc_length);
                        score += idf * tf;
                    }
                    results.push_back({current_doc, score});
                }
                ++current_doc; // Increment to the next doc
            }
            else
            {
                // Move to the largest doc_id found across the lists to continue
                current_doc = max_doc;
            }

            // Exit condition: stop when the current doc_id exceeds total_docs
            if (current_doc >= total_docs || max_doc == -1)
                break;
        }

        return results;
    }

    std::vector<SearchResult> disjunctiveSearch(std::vector<InvertedList> &lists)
    {
        std::cout << "Disjunctive search..." << std::endl;
        std::vector<SearchResult> results;
        std::vector<int> doc_ids(lists.size(), 0);
        std::vector<int> freqs(lists.size(), 0);
        std::priority_queue<std::pair<int, int>> pq;
        std::cout << "Disjunctive search initialized." << std::endl;
        for (size_t i = 0; i < lists.size(); ++i)
        {
            if (lists[i].next(doc_ids[i], freqs[i]))
            {
                pq.push({-doc_ids[i], i});
            }
        }

        while (!pq.empty())
        {
            int doc_id = -pq.top().first;
            int list_index = pq.top().second;
            pq.pop();

            double score = 0;

            // Check if doc_id is within the range of doc_lengths
            if (doc_id < 0 || doc_id >= doc_lengths.size())
            {
                std::cerr << "Invalid doc_id: " << doc_id << std::endl;
                continue;
            }

            int doc_length = doc_lengths[doc_id];

            for (size_t i = 0; i < lists.size(); ++i)
            {
                if (doc_ids[i] == doc_id)
                {
                    double idf = computeIDF(lists[i].getSize());
                    double tf = computeTF(freqs[i], doc_length);
                    score += idf * tf;

                    if (lists[i].next(doc_ids[i], freqs[i]))
                    {
                        pq.push({-doc_ids[i], i});
                    }
                }
            }

            results.push_back({doc_id, score});
        }

        return results;
    }
};

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

size_t varbyteEncodedSize(int value)
{
    size_t size = 0;
    do
    {
        value >>= 7;
        size++;
    } while (value > 0);
    return size;
}

int varbyteDecode(const uint8_t *data, size_t max_size, size_t &bytes_read)
{
    int value = 0;
    int shift = 0;
    bytes_read = 0;

    for (size_t i = 0; i < max_size; ++i)
    {
        uint8_t byte = data[i];
        value |= (byte & 0x7F) << shift; // Use lower 7 bits
        shift += 7;
        bytes_read++;

        if (byte & 0x80) // Check if this is the last byte
        {
            break;
        }
    }

    return value;
}

int main()
{
    SearchEngine engine(LEXICON_FILE,
                        INDEX_FILE,
                        DOC_INFO_FILE,
                        BLOCK_INFO_FILE,
                        ORIGINAL_TAR_GZ);

    std::string query;
    bool conjunctive;
    while (true)
    {
        std::cout << "Enter your search query (or 'q' to exit): ";
        std::getline(std::cin, query);
        if (query == "q")
            break;

        std::cout << "Enter search mode (0 for disjunctive, 1 for conjunctive): ";
        std::cin >> conjunctive;
        std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

        auto results = engine.search(query, conjunctive);

        std::cout << "Top 10 results:" << std::endl;
        for (const auto &result : results)
        {
            std::cout << "Doc ID: " << result.doc_id << ", Score: " << result.score << std::endl;
            // find line position of the original file and print the content
            std::string content = engine.getOriginalFileContent(result.doc_id);
            std::cout << content << std::endl;
        }
    }

    return 0;
}
