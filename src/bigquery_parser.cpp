
#include "duckdb.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"

#include "bigquery_client.hpp"
#include "bigquery_info.hpp"
#include "bigquery_parser.hpp"
#include "bigquery_settings.hpp"

namespace duckdb {
namespace bigquery {

unordered_map<string, string> parse_bigquery_options(const string &options_str) {
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

ParserExtensionParseResult bigquery_parse(ParserExtensionInfo *info, const string &query) {
    if (!BigquerySettings::ExperimentalEnableBigqueryOptions()) {
        return ParserExtensionParseResult();
    }

    std::regex options_regex( //
        R"(^(CREATE[\s\S]*?(?:TABLE|SCHEMA)[\s\S]*?)OPTIONS\s*\(([\s\S]*?)\)([\s\S]*);?)",
        std::regex_constants::icase);
    std::smatch options_match;

    if (std::regex_search(query, options_match, options_regex)) {
        string create_stmt = options_match[1].str();
        string options_str = options_match[2].str();
        string rest_of_stmt = options_match[3].str();

        // Extract options
        auto options_map = parse_bigquery_options(options_str);
        string cleaned_query = create_stmt + " " + rest_of_stmt + ";";

        Parser parser;
        parser.ParseQuery(cleaned_query);

        auto statements = std::move(parser.statements);
        if (options_map.size() > 0 && statements.size() != 1) {
            return ParserExtensionParseResult("OPTIONS in CREATE statement is only supported for single statements");
        }

        for (auto &statement : statements) {
            if (statement->type == StatementType::CREATE_STATEMENT) {
                auto &create_stmt = statement->Cast<CreateStatement>();
                auto *info_ptr = create_stmt.info.get();

                if (auto *schema_info_ptr = dynamic_cast<CreateSchemaInfo *>(info_ptr)) {
                    auto &schema_info = *schema_info_ptr;
                    create_stmt.info = make_uniq<BigqueryCreateSchemaInfo>(schema_info, options_map);
                } else if (auto *table_info_ptr = dynamic_cast<CreateTableInfo *>(info_ptr)) {
                    auto &table_info = *table_info_ptr;
                    create_stmt.info = make_uniq<BigqueryCreateTableInfo>(table_info, options_map);
                } else if (auto *view_info_ptr = dynamic_cast<CreateViewInfo *>(info_ptr)) {
                    auto &view_info = *view_info_ptr;
                    create_stmt.info = make_uniq<BigqueryCreateViewInfo>(view_info, options_map);
                }
            }
        }

        return ParserExtensionParseResult(
            make_uniq_base<ParserExtensionParseData, BigqueryParseData>(std::move(statements[0]), options_map));
    }

    return ParserExtensionParseResult();
}

ParserExtensionPlanResult bigquery_plan(ParserExtensionInfo *,
                                        ClientContext &context,
                                        unique_ptr<ParserExtensionParseData> parse_data) {
    auto bigquery_state = make_shared_ptr<BigqueryState>(std::move(parse_data));
    context.registered_state->Remove("bigquery");
    context.registered_state->Insert("bigquery", bigquery_state);
    throw BinderException("nope");
}

BoundStatement bigquery_bind(ClientContext &context,
                             Binder &binder,
                             OperatorExtensionInfo *info,
                             SQLStatement &statement) {
    switch (statement.type) {
    case StatementType::EXTENSION_STATEMENT: {
        auto &extension_statement = dynamic_cast<ExtensionStatement &>(statement);
        if (extension_statement.extension.parse_function == bigquery_parse) {
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
                    create_stmt.info = make_uniq<BigqueryCreateTableInfo>(*table_info_ptr, options_map);
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
