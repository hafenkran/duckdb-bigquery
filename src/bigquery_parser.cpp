
#include "duckdb.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"

#include "bigquery_client.hpp"
#include "bigquery_info.hpp"
#include "bigquery_parser.hpp"
#include "bigquery_settings.hpp"

#include <cctype>
#include <regex>

namespace duckdb {
namespace bigquery {

struct BigqueryClauseParseResult {
    string cleaned_query;
    vector<string> partition_by;
    vector<string> cluster_by;
    unordered_map<string, string> options;
};

static bool IsIdentifierChar(char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

static char LowerChar(char c) {
    return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
}

static unordered_map<string, string> ParseBigqueryOptions(const string &options_str) {
    unordered_map<string, string> options;
    std::regex option_regex(R"((\w+)\s*=\s*('[^']*'|"[^"]*"|\w+))");

    auto options_begin = std::sregex_iterator(options_str.begin(), options_str.end(), option_regex);
    auto options_end = std::sregex_iterator();

    for (std::sregex_iterator i = options_begin; i != options_end; ++i) {
        std::smatch match = *i;
        string key = match[1];
        string value = match[2];

        if ((value.front() == '\'' && value.back() == '\'') || (value.front() == '"' && value.back() == '"')) {
            value = value.substr(1, value.size() - 2);
        }
        options[key] = value;
    }

    return options;
}

static bool MatchKeyword(const string &query, idx_t position, const string &keyword) {
    if (position + keyword.size() > query.size()) {
        return false;
    }
    if (position > 0 && IsIdentifierChar(query[position - 1])) {
        return false;
    }
    for (idx_t i = 0; i < keyword.size(); i++) {
        if (LowerChar(query[position + i]) != LowerChar(keyword[i])) {
            return false;
        }
    }
    idx_t end_pos = position + keyword.size();
    if (end_pos < query.size() && IsIdentifierChar(query[end_pos])) {
        return false;
    }
    return true;
}

static idx_t SkipWhitespace(const string &query, idx_t position) {
    while (position < query.size() && std::isspace(static_cast<unsigned char>(query[position]))) {
        position++;
    }
    return position;
}

static string TrimWhitespace(const string &input) {
    idx_t start = 0;
    while (start < input.size() && std::isspace(static_cast<unsigned char>(input[start]))) {
        start++;
    }
    if (start == input.size()) {
        return "";
    }
    idx_t end = input.size() - 1;
    while (end > start && std::isspace(static_cast<unsigned char>(input[end]))) {
        end--;
    }
    return input.substr(start, end - start + 1);
}

static string StripOuterParentheses(const string &input) {
    auto trimmed = TrimWhitespace(input);
    if (trimmed.size() < 2 || trimmed.front() != '(' || trimmed.back() != ')') {
        return trimmed;
    }
    int depth = 0;
    char quote = 0;
    for (idx_t i = 0; i < trimmed.size(); i++) {
        char c = trimmed[i];
        if (quote) {
            if (c == quote && (i == 0 || trimmed[i - 1] != '\\')) {
                quote = 0;
            }
            continue;
        }
        if (c == '\'' || c == '"' || c == '`') {
            quote = c;
            continue;
        }
        if (c == '(') {
            depth++;
        } else if (c == ')') {
            depth--;
            if (depth == 0 && i + 1 != trimmed.size()) {
                return trimmed;
            }
        }
    }
    if (depth == 0) {
        return TrimWhitespace(trimmed.substr(1, trimmed.size() - 2));
    }
    return trimmed;
}

static vector<string> SplitTopLevelList(const string &input) {
    vector<string> parts;
    string current;
    int depth = 0;
    char quote = 0;
    for (idx_t i = 0; i < input.size(); i++) {
        char c = input[i];
        if (quote) {
            current.push_back(c);
            if (c == quote && (i == 0 || input[i - 1] != '\\')) {
                quote = 0;
            }
            continue;
        }
        if (c == '\'' || c == '"' || c == '`') {
            quote = c;
            current.push_back(c);
            continue;
        }
        if (c == '(') {
            depth++;
            current.push_back(c);
            continue;
        }
        if (c == ')') {
            if (depth > 0) {
                depth--;
            }
            current.push_back(c);
            continue;
        }
        if (c == ',' && depth == 0) {
            auto trimmed = TrimWhitespace(current);
            if (!trimmed.empty()) {
                parts.push_back(trimmed);
            }
            current.clear();
            continue;
        }
        current.push_back(c);
    }
    auto trimmed = TrimWhitespace(current);
    if (!trimmed.empty()) {
        parts.push_back(trimmed);
    }
    return parts;
}

static idx_t FindMatchingParen(const string &query, idx_t open_pos) {
    int depth = 0;
    char quote = 0;
    for (idx_t i = open_pos; i < query.size(); i++) {
        char c = query[i];
        if (quote) {
            if (c == quote && (i == 0 || query[i - 1] != '\\')) {
                quote = 0;
            }
            continue;
        }
        if (c == '\'' || c == '"' || c == '`') {
            quote = c;
            continue;
        }
        if (c == '(') {
            depth++;
            continue;
        }
        if (c == ')') {
            depth--;
            if (depth == 0) {
                return i;
            }
        }
    }
    return query.size();
}

static idx_t FindClauseEnd(const string &query, idx_t position) {
    int depth = 0;
    char quote = 0;
    for (idx_t i = position; i < query.size(); i++) {
        char c = query[i];
        if (quote) {
            if (c == quote && (i == 0 || query[i - 1] != '\\')) {
                quote = 0;
            }
            continue;
        }
        if (c == '\'' || c == '"' || c == '`') {
            quote = c;
            continue;
        }
        if (c == '(') {
            depth++;
            continue;
        }
        if (c == ')') {
            if (depth > 0) {
                depth--;
            }
            continue;
        }
        if (depth == 0) {
            if (c == ';') {
                return i;
            }
            if (MatchKeyword(query, i, "PARTITION") || MatchKeyword(query, i, "CLUSTER") ||
                MatchKeyword(query, i, "OPTIONS") || MatchKeyword(query, i, "AS")) {
                return i;
            }
        }
    }
    return query.size();
}

static BigqueryClauseParseResult ExtractBigqueryClauses(const string &query) {
    BigqueryClauseParseResult result;
    vector<pair<idx_t, idx_t>> remove_ranges;

    int depth = 0;
    char quote = 0;
    for (idx_t i = 0; i < query.size(); i++) {
        char c = query[i];
        if (quote) {
            if (c == quote && (i == 0 || query[i - 1] != '\\')) {
                quote = 0;
            }
            continue;
        }
        if (c == '\'' || c == '"' || c == '`') {
            quote = c;
            continue;
        }
        if (c == '(') {
            depth++;
            continue;
        }
        if (c == ')') {
            if (depth > 0) {
                depth--;
            }
            continue;
        }
        if (depth != 0) {
            continue;
        }
        bool is_partition = MatchKeyword(query, i, "PARTITION");
        bool is_cluster = MatchKeyword(query, i, "CLUSTER");
        bool is_options = MatchKeyword(query, i, "OPTIONS");
        if (!is_partition && !is_cluster && !is_options) {
            continue;
        }
        idx_t keyword_start = i;
        if (is_options) {
            idx_t keyword_end = i + 7;
            idx_t open_paren = SkipWhitespace(query, keyword_end);
            if (open_paren >= query.size() || query[open_paren] != '(') {
                i = keyword_end;
                continue;
            }
            idx_t close_paren = FindMatchingParen(query, open_paren);
            if (close_paren <= open_paren || close_paren >= query.size()) {
                i = close_paren;
                continue;
            }
            string options_str = query.substr(open_paren + 1, close_paren - open_paren - 1);
            result.options = ParseBigqueryOptions(options_str);
            remove_ranges.emplace_back(keyword_start, close_paren + 1);
            i = close_paren;
            continue;
        }

        idx_t keyword_end = i + (is_partition ? 9 : 7);
        idx_t by_pos = SkipWhitespace(query, keyword_end);
        if (!MatchKeyword(query, by_pos, "BY")) {
            i = keyword_end;
            continue;
        }
        idx_t content_start = SkipWhitespace(query, by_pos + 2);
        idx_t content_end = FindClauseEnd(query, content_start);
        if (content_end <= content_start) {
            i = content_end;
            continue;
        }

        auto content = StripOuterParentheses(query.substr(content_start, content_end - content_start));
        auto items = SplitTopLevelList(content);
        if (is_partition) {
            result.partition_by = std::move(items);
        } else {
            result.cluster_by = std::move(items);
        }

        remove_ranges.emplace_back(keyword_start, content_end);
        if (content_end > 0) {
            i = content_end - 1;
        } else {
            i = content_end;
        }
    }

    if (remove_ranges.empty()) {
        result.cleaned_query = query;
        return result;
    }

    string cleaned;
    idx_t last = 0;
    for (auto &range : remove_ranges) {
        if (range.first > last) {
            cleaned += query.substr(last, range.first - last);
            cleaned += " ";
        }
        last = range.second;
    }
    cleaned += query.substr(last);
    result.cleaned_query = cleaned;
    return result;
}

ParserExtensionParseResult BigqueryParse(ParserExtensionInfo *info, const string &query) {
    if (!BigquerySettings::ExperimentalEnableSqlParser()) {
        return ParserExtensionParseResult();
    }

    auto clause_result = ExtractBigqueryClauses(query);
    if (clause_result.partition_by.empty() && clause_result.cluster_by.empty() && clause_result.options.empty()) {
        return ParserExtensionParseResult();
    }

    Parser parser;
    parser.ParseQuery(clause_result.cleaned_query);

    auto statements = std::move(parser.statements);
    if (statements.size() != 1) {
        if (!clause_result.options.empty()) {
            return ParserExtensionParseResult("OPTIONS in CREATE statement is only supported for single statements");
        }
        return ParserExtensionParseResult(
            "BigQuery CREATE TABLE with PARTITION/CLUSTER supports only single statements");
    }

    for (auto &statement : statements) {
        if (statement->type == StatementType::CREATE_STATEMENT) {
            auto &create_stmt = statement->Cast<CreateStatement>();
            auto *info_ptr = create_stmt.info.get();
            if (auto *table_info_ptr = dynamic_cast<CreateTableInfo *>(info_ptr)) {
                auto &table_info = *table_info_ptr;
                create_stmt.info = make_uniq<BigqueryCreateTableInfo>(table_info, clause_result.options);
                auto *bq_table_info = dynamic_cast<BigqueryCreateTableInfo *>(create_stmt.info.get());
                if (bq_table_info) {
                    bq_table_info->partition_by = clause_result.partition_by;
                    bq_table_info->cluster_by = clause_result.cluster_by;
                }
            } else if (auto *schema_info_ptr = dynamic_cast<CreateSchemaInfo *>(info_ptr)) {
                auto &schema_info = *schema_info_ptr;
                create_stmt.info = make_uniq<BigqueryCreateSchemaInfo>(schema_info, clause_result.options);
            } else if (auto *view_info_ptr = dynamic_cast<CreateViewInfo *>(info_ptr)) {
                auto &view_info = *view_info_ptr;
                create_stmt.info = make_uniq<BigqueryCreateViewInfo>(view_info, clause_result.options);
            }
        }
    }

    return ParserExtensionParseResult(
        make_uniq_base<ParserExtensionParseData, BigqueryParseData>(std::move(statements[0]),
                                                                    clause_result.options,
                                                                    clause_result.partition_by,
                                                                    clause_result.cluster_by));
}

ParserExtensionPlanResult BigqueryPlan(ParserExtensionInfo *,
                                       ClientContext &context,
                                       unique_ptr<ParserExtensionParseData> parse_data) {
    auto bigquery_state = make_shared_ptr<BigqueryState>(std::move(parse_data));
    context.registered_state->Remove("bigquery");
    context.registered_state->Insert("bigquery", bigquery_state);
    throw BinderException("nope");
}

BoundStatement BigqueryBind(ClientContext &context,
                            Binder &binder,
                            OperatorExtensionInfo *info,
                            SQLStatement &statement) {
    switch (statement.type) {
    case StatementType::EXTENSION_STATEMENT: {
        auto &extension_statement = dynamic_cast<ExtensionStatement &>(statement);
        if (extension_statement.extension.parse_function == BigqueryParse) {
            auto lookup = context.registered_state->Get<BigqueryState>("bigquery");
            if (!lookup) {
                throw BinderException("Bigquery registered state not found");
            }

            auto bigquery_state = dynamic_cast<BigqueryState *>(lookup.get());
            if (!bigquery_state) {
                throw BinderException("Bigquery registered state is not of type BigqueryState");
            }
            auto bigquery_parse_data = dynamic_cast<BigqueryParseData *>(bigquery_state->parse_data.get());
            if (!bigquery_parse_data) {
                throw BinderException("Bigquery parse data invalid");
            }

            auto &parsed_statement = bigquery_parse_data->statement;
            auto &options_map = bigquery_parse_data->options;
            auto &partition_by = bigquery_parse_data->partition_by;
            auto &cluster_by = bigquery_parse_data->cluster_by;

            if (parsed_statement->type == StatementType::CREATE_STATEMENT) {
                auto &create_stmt = parsed_statement->Cast<CreateStatement>();
                auto &info = *create_stmt.info;

                if (options_map.size() > 0) {
                    string &catalog_name = info.catalog;
                    auto &database_manager = DatabaseManager::Get(context);
                    auto database = database_manager.GetDatabase(context, catalog_name);
                    if (!database) {
                        throw BinderException("OPTIONS clause used on non-BigQuery catalog %s", catalog_name);
                    }
                }

                if (auto *schema_info_ptr = dynamic_cast<CreateSchemaInfo *>(&info)) {
                    create_stmt.info = make_uniq<BigqueryCreateSchemaInfo>(*schema_info_ptr, options_map);
                } else if (auto *table_info_ptr = dynamic_cast<CreateTableInfo *>(&info)) {
                    if (auto *bq_table_info_ptr = dynamic_cast<BigqueryCreateTableInfo *>(&info)) {
                        bq_table_info_ptr->options = options_map;
                        bq_table_info_ptr->partition_by = partition_by;
                        bq_table_info_ptr->cluster_by = cluster_by;
                    } else {
                        create_stmt.info = make_uniq<BigqueryCreateTableInfo>(*table_info_ptr, options_map);
                        auto *bq_table_info = dynamic_cast<BigqueryCreateTableInfo *>(create_stmt.info.get());
                        if (bq_table_info) {
                            bq_table_info->partition_by = partition_by;
                            bq_table_info->cluster_by = cluster_by;
                        }
                    }
                } else if (auto *view_info_ptr = dynamic_cast<CreateViewInfo *>(&info)) {
                    create_stmt.info = make_uniq<BigqueryCreateViewInfo>(*view_info_ptr, options_map);
                }
            }

            auto bigquery_binder = Binder::CreateBinder(context, &binder);
            return bigquery_binder->Bind(*parsed_statement);
        }
    }
    default:
        return {};
    }
}

} // namespace bigquery
} // namespace duckdb
