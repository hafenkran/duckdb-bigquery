#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "bigquery_utils.hpp"

#include <stdexcept>

using namespace duckdb;
using namespace duckdb::bigquery;

namespace {

static void CheckConfig(const BigqueryConfig &config,
                        const string &project,
                        const string &dataset = "",
                        const string &billing_project = "",
                        const string &api_endpoint = "",
                        const string &grpc_endpoint = "") {
    CHECK(config.project_id == project);
    CHECK(config.dataset_id == dataset);
    CHECK(config.billing_project_id == billing_project);
    CHECK(config.api_endpoint == api_endpoint);
    CHECK(config.grpc_endpoint == grpc_endpoint);
    CHECK(config.secret_name.empty());
}

} // namespace

TEST_CASE("Project shorthand connections match named project connections", "[bigquery][config]") {
    const auto named = BigqueryConfig::FromDSN("project=review-project");
    CheckConfig(named, "review-project");
    const char *cases[] = {"review-project", "projects/review-project"};
    for (const auto *dsn : cases) {
        CAPTURE(dsn);
        const auto shorthand = BigqueryConfig::FromDSN(dsn);
        CheckConfig(shorthand, "review-project");
        CHECK(shorthand.project_id == named.project_id);
        CHECK(shorthand.dataset_id == named.dataset_id);
        CHECK(shorthand.BillingProject() == "review-project");
    }
}

TEST_CASE("Dataset shorthand connections match named dataset connections", "[bigquery][config]") {
    const auto named = BigqueryConfig::FromDSN("project=review-project dataset=review_dataset");
    CheckConfig(named, "review-project", "review_dataset");
    const char *cases[] = {"review-project.review_dataset",
                           "review-project:review_dataset",
                           "projects/review-project/datasets/review_dataset"};
    for (const auto *dsn : cases) {
        CAPTURE(dsn);
        const auto shorthand = BigqueryConfig::FromDSN(dsn);
        CheckConfig(shorthand, "review-project", "review_dataset");
        CHECK(shorthand.project_id == named.project_id);
        CHECK(shorthand.dataset_id == named.dataset_id);
        CHECK(shorthand.BillingProject() == "review-project");
    }
}

TEST_CASE("Named connections retain every option regardless of the first key", "[bigquery][config]") {
    const char *cases[] = {
        "project=review-project dataset=review_dataset billing_project=review-billing "
        "api_endpoint=http://127.0.0.1:9050 grpc_endpoint=127.0.0.1:9060",
        "dataset=review_dataset billing_project=review-billing api_endpoint=http://127.0.0.1:9050 "
        "grpc_endpoint=127.0.0.1:9060 project=review-project",
        "api_endpoint=http://127.0.0.1:9050 grpc_endpoint=127.0.0.1:9060 billing_project=review-billing "
        "dataset=review_dataset project=review-project"};
    for (const auto *dsn : cases) {
        CAPTURE(dsn);
        const auto config = BigqueryConfig::FromDSN(dsn);
        CheckConfig(config,
                    "review-project",
                    "review_dataset",
                    "review-billing",
                    "http://127.0.0.1:9050",
                    "127.0.0.1:9060");
        CHECK(config.BillingProject() == "review-billing");
    }
}

TEST_CASE("Shorthand connections retain following named options", "[bigquery][config]") {
    const char *cases[] = {"review-project", "review-project.review_dataset"};
    for (const auto *prefix : cases) {
        CAPTURE(prefix);
        const auto config = BigqueryConfig::FromDSN(
            string(prefix) + " dataset=review_dataset billing_project=review-billing "
                             "api_endpoint=http://127.0.0.1:9050?audience=bigquery grpc_endpoint=127.0.0.1:9060");
        CheckConfig(config,
                    "review-project",
                    "review_dataset",
                    "review-billing",
                    "http://127.0.0.1:9050?audience=bigquery",
                    "127.0.0.1:9060");
        CHECK(config.BillingProject() == "review-billing");
    }
}

TEST_CASE("Later named options override shorthand and earlier named options", "[bigquery][config]") {
    const char *prefixes[] = {"old-project.old_dataset", "project=old-project dataset=old_dataset"};
    for (const auto *prefix : prefixes) {
        CAPTURE(prefix);
        const auto config = BigqueryConfig::FromDSN(
            string(prefix) + " billing_project=old-billing api_endpoint=http://127.0.0.1:1 grpc_endpoint=127.0.0.1:2 "
                             "project=review-project dataset=review_dataset billing_project=review-billing "
                             "api_endpoint=http://127.0.0.1:9050 grpc_endpoint=127.0.0.1:9060");
        CheckConfig(config,
                    "review-project",
                    "review_dataset",
                    "review-billing",
                    "http://127.0.0.1:9050",
                    "127.0.0.1:9060");
        CHECK(config.BillingProject() == "review-billing");
    }
}

TEST_CASE("Empty optional connection values retain their existing defaults", "[bigquery][config]") {
    const char *prefixes[] = {"review-project.review_dataset", "project=review-project dataset=review_dataset"};
    for (const auto *prefix : prefixes) {
        CAPTURE(prefix);
        const auto config =
            BigqueryConfig::FromDSN(string(prefix) + " dataset= billing_project= api_endpoint= grpc_endpoint=");
        CheckConfig(config, "review-project");
        CHECK(config.BillingProject() == "review-project");
    }
}

TEST_CASE("ATTACH connections reject table identifiers", "[bigquery][config]") {
    const char *cases[] = {"review-project.review_dataset.target",
                           "review-project:review_dataset.target",
                           "projects/review-project/datasets/review_dataset/tables/target"};
    for (const auto *dsn : cases) {
        CAPTURE(dsn);
        CHECK_THROWS_WITH(BigqueryConfig::FromDSN(dsn), "Table ID is not supported in the connection string");
    }
}

TEST_CASE("ATTACH shorthand rejects malformed project and dataset IDs", "[bigquery][config]") {
    CHECK_THROWS_WITH(BigqueryConfig::FromDSN("review-project.review_dataset/target"),
                      "Invalid dataset ID in connection string: review_dataset/target");
    CHECK_THROWS_WITH(BigqueryConfig::FromDSN("review-project\treview_dataset"),
                      "Invalid project ID in connection string: review-project\treview_dataset");
}

TEST_CASE("Invalid connection strings fail before a client is needed", "[bigquery][config]") {
    const char *cases[] = {"",
                           "review-project.",
                           "review-project..review_dataset",
                           "project=",
                           "dataset=review_dataset",
                           "review-project project=",
                           "project=review-project unknown=value",
                           "review-project unknown=value",
                           "unknown=value project=review-project",
                           "project=review-project =value",
                           "review-project =value",
                           "project=review-project unkeyed",
                           "review-project unkeyed",
                           "review-project  dataset=review_dataset"};
    for (const auto *dsn : cases) {
        CAPTURE(dsn);
        CHECK_THROWS_AS(BigqueryConfig::FromDSN(dsn), std::invalid_argument);
    }
}
