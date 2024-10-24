#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <queue>
#include <cmath>
#include <sstream>

const int BLOCK_SIZE = 64 * 1024; // 64KB
const std::string LEXICON_FILE = "final_sorted_lexicon.txt";
const std::string INDEX_FILE = "final_sorted_index.bin";
const std::string DOC_LENGTHS_FILE = "document_term_count.txt";
const std::string BLOCK_INFO_FILE = "final_block_info2.txt";

// parameters
const double k1 = 1.2;
const double b = 0.75;

// decode function
int varbyteDecode(const std::vector<uint8_t> &encoded, size_t &pos);

struct LexiconEntry
{
    int term_id;
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
public:
    InvertedList(std::ifstream &index_file, int64_t start_pos, int64_t bytes_size)
        : index_file_(index_file), start_pos_(start_pos), bytes_size_(bytes_size), current_pos_(0)
    {
        loadNextBlock();
    }

    bool next(int &doc_id, int &freq)
    {
        if (current_pos_ >= current_block_.size())
        {
            if (!loadNextBlock())
                return false;
        }
        doc_id += varbyteDecode(current_block_, current_pos_);
        freq = varbyteDecode(current_block_, current_pos_);
        return true;
    }

    int64_t getSize() const { return bytes_size_; }

private:
    std::ifstream &index_file_;
    int64_t start_pos_;
    int64_t bytes_size_;
    size_t current_pos_;
    std::vector<uint8_t> current_block_;

    bool loadNextBlock()
    {
        int64_t remaining = bytes_size_ - (start_pos_ + current_block_.size());
        if (remaining <= 0)
            return false;
        int64_t block_size = std::min(static_cast<int64_t>(BLOCK_SIZE), remaining);
        current_block_.resize(block_size);
        index_file_.seekg(start_pos_ + current_block_.size());
        index_file_.read(reinterpret_cast<char *>(current_block_.data()), block_size);
        current_pos_ = 0;
        return true;
    }
};

class SearchEngine
{
private: // private members
    std::unordered_map<std::string, LexiconEntry> lexicon;
    std::vector<std::pair<std::string, int>> block;
    std::unordered_map<int, std::string> term_id_to_word;
    std::ifstream index_file;
    std::ifstream doc_lengths_file;
    int total_docs;
    double avg_doc_length;

    int getDocLength(int doc_id)
    {
        doc_lengths_file.seekg(doc_id * sizeof(int));
        int length;
        doc_lengths_file.read(reinterpret_cast<char *>(&length), sizeof(int));
        return length;
    }

public: // public members
    SearchEngine(const std::string &lexicon_file, const std::string &index_file,
                 const std::string &doc_lengths_file, const std::string &block_info_file)
        : index_file(index_file, std::ios::binary), doc_lengths_file(doc_lengths_file, std::ios::binary)
    {
        loadLexicon(lexicon_file);
        loadBlockInfo(block_info_file, term_id_to_word);
    }

    void loadLexicon(const std::string &lexicon_file)
    {
        std::ifstream lex_file(lexicon_file);
        std::unordered_map<int, std::string> term_id_to_word;
        std::string term;
        LexiconEntry entry;
        std::cout << "Loading lexicon..." << std::endl;
        while (lex_file >> term >> entry.term_id >> entry.start_position >> entry.bytes_size) // tested
        {
            lexicon[term] = entry;
            term_id_to_word[entry.term_id] = term;
        }
        std::cout << "Lexicon loaded." << std::endl;
    }

    void loadBlockInfo(const std::string &block_info_file, const std::unordered_map<int, std::string> &term_id_to_word)
    {
        std::cout << "Loading block info..." << std::endl;
        std::ifstream block_info(block_info_file);
        int ind;
        while (block_info >> ind)
        {
            int last_term_id;
            int block_close_pos;
            block_info >> last_term_id >> block_close_pos;
            std::string last_term = term_id_to_word.at(last_term_id);
            block.push_back({last_term, block_close_pos});
        }
        std::cout << "Block info loaded." << std::endl;
    }

    std::vector<SearchResult> search(const std::string &query, bool conjunctive)
    {
        // process the query
        std::vector<std::string> terms = processQuery(query);
        std::vector<InvertedList> lists;
        // find the inverted lists for the terms
        for (const auto &term : terms)
        {
            if (lexicon.find(term) != lexicon.end())
            {
                const auto &entry = lexicon[term];
                lists.emplace_back(index_file, entry.start_position, entry.bytes_size);
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
        std::vector<SearchResult> results;
        int current_doc = 0;
        std::vector<int> doc_ids(lists.size(), 0);
        std::vector<int> freqs(lists.size(), 0);

        while (true)
        {
            bool all_equal = true;
            int max_doc = 0;

            for (size_t i = 0; i < lists.size(); ++i)
            {
                while (doc_ids[i] < current_doc && lists[i].next(doc_ids[i], freqs[i]))
                {
                }
                if (doc_ids[i] > max_doc)
                    max_doc = doc_ids[i];
                if (doc_ids[i] != current_doc)
                    all_equal = false;
            }

            if (all_equal)
            {
                double score = 0;
                int doc_length = getDocLength(current_doc);
                for (size_t i = 0; i < lists.size(); ++i)
                {
                    double idf = computeIDF(lists[i].getSize());
                    double tf = computeTF(freqs[i], doc_length);
                    score += idf * tf;
                }
                results.push_back({current_doc, score});
                ++current_doc;
            }
            else
            {
                current_doc = max_doc;
            }

            if (current_doc >= total_docs)
                break;
        }

        return results;
    }

    std::vector<SearchResult> disjunctiveSearch(std::vector<InvertedList> &lists)
    {
        std::vector<SearchResult> results;
        std::vector<int> doc_ids(lists.size(), 0);
        std::vector<int> freqs(lists.size(), 0);
        std::priority_queue<std::pair<int, int>> pq;

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
            int doc_length = getDocLength(doc_id);

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

// varbyte decode function
int varbyteDecode(const std::vector<uint8_t> &encoded, size_t &pos)
{
    int value = 0;
    int shift = 0;
    uint8_t byte;
    do
    {
        byte = encoded[pos++];
        value |= (byte & 0x7F) << shift;
        shift += 7;
    } while (byte & 0x80);
    return value;
}

int main()
{
    SearchEngine engine(LEXICON_FILE,
                        INDEX_FILE,
                        DOC_LENGTHS_FILE,
                        BLOCK_INFO_FILE);

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
        }
    }

    return 0;
}
