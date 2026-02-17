from dagster_dataform.resources import DataformRepositoryResource
from dagster_dataform.utils import get_epoch_time_ago
import pytest
from google.cloud import dataform_v1
from dagster import AssetSpec, AssetChecksDefinition


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "test-commitish",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_initialization(mock_dataform_client):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    assert resource is not None
    assert resource.project_id == "test-project"
    assert resource.location == "us-central1"
    assert resource.repository_id == "test-repo"
    assert resource.environment == "dev"
    assert resource.sensor_minimum_interval_seconds == 120


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "test-commitish",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_create_compilation_result(mock_dataform_client):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )
    compilation_result = resource.create_compilation_result(
        git_commitish="test-commitish",
        default_database="test-database",
        default_schema="test-schema",
        default_location="us-central1",
        assertion_schema="test-assertion-schema",
    )

    expected_compilation_result = dataform_v1.CompilationResult(
        git_commitish="test-commitish",
        name="test-compilation-result",
        code_compilation_config=dataform_v1.CodeCompilationConfig(
            default_database="test-database",
            default_schema="test-schema",
            default_location="us-central1",
            assertion_schema="test-assertion-schema",
        ),
    )

    assert compilation_result == expected_compilation_result


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "test-commitish",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_get_latest_compilation_result_name_wrong_environment(
    mock_dataform_client,
):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    compilation_result = resource.get_latest_compilation_result_name()

    assert compilation_result is None


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "dev",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_get_latest_compilation_result_name_correct_environment(
    mock_dataform_client,
):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    compilation_result = resource.get_latest_compilation_result_name()

    assert compilation_result is not None
    assert compilation_result == "test-compilation-result"


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "dev",
            "default_database": "production-project",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_get_latest_compilation_result_name_wrong_database(
    mock_dataform_client,
):
    """A compilation for a different default_database should not be returned when filtering by database."""
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    # The mock has default_database="production-project", but we're looking for "staging-project"
    compilation_result = resource.get_latest_compilation_result_name(
        default_database="staging-project"
    )

    assert compilation_result is None


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "dev",
            "default_database": "staging-project",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_get_latest_compilation_result_name_matching_database(
    mock_dataform_client,
):
    """A compilation for the correct default_database should be returned."""
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    compilation_result = resource.get_latest_compilation_result_name(
        default_database="staging-project"
    )

    assert compilation_result is not None
    assert compilation_result == "test-compilation-result"


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "dev",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_query_compilation_result(mock_dataform_client):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    compilation_result_actions = resource.query_compilation_result()

    assert compilation_result_actions is not None
    assert len(compilation_result_actions) == 2
    # Access the attributes through the mock response object
    assert hasattr(compilation_result_actions[0], "target")
    assert hasattr(compilation_result_actions[0].target, "name")
    assert hasattr(compilation_result_actions[0].target, "schema")
    assert hasattr(compilation_result_actions[0].target, "database")


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "dev",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_get_latest_workflow_invocations(
    mock_dataform_client,
):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    workflow_invocations = resource.get_latest_workflow_invocations(minutes_ago=10)

    expected_request = dataform_v1.ListWorkflowInvocationsRequest(
        parent="projects/test-project/locations/us-central1/repositories/test-repo",
        page_size=1000,
        filter=f"invocation_timing.start_time.seconds > {get_epoch_time_ago(minutes=10)}",
    )

    mock_dataform_client.list_workflow_invocations.assert_called_once_with(
        request=expected_request
    )

    assert workflow_invocations is not None
    assert len(workflow_invocations) == 1  # pyright: ignore[reportArgumentType]
    assert workflow_invocations[0].name == "test-workflow-invocation"  # pyright: ignore[reportIndexIssue]


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "dev",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_query_workflow_invocation(mock_dataform_client):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    workflow_invocation = resource.query_workflow_invocation(
        name="test-workflow-invocation"
    )

    expected_request = dataform_v1.QueryWorkflowInvocationActionsRequest(
        name="test-workflow-invocation",
    )

    mock_dataform_client.query_workflow_invocation_actions.assert_called_once_with(
        request=expected_request
    )

    assert workflow_invocation is not None
    assert hasattr(workflow_invocation, "workflow_invocation_actions")
    assert len(workflow_invocation.workflow_invocation_actions) == 1
    assert (
        workflow_invocation.workflow_invocation_actions[0].state
        == dataform_v1.WorkflowInvocationAction.State.SUCCEEDED
    )
    assert (
        workflow_invocation.workflow_invocation_actions[0].target.name == "test_asset"
    )
    assert (
        workflow_invocation.workflow_invocation_actions[0].target.schema
        == "test_schema"
    )
    assert (
        workflow_invocation.workflow_invocation_actions[0].target.database
        == "test-database"
    )


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "dev",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_create_workflow_invocation(mock_dataform_client):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    workflow_invocation = resource.create_workflow_invocation(
        compilation_result_name="test-compilation-result"
    )

    expected_request = dataform_v1.CreateWorkflowInvocationRequest(
        parent="projects/test-project/locations/us-central1/repositories/test-repo",
        workflow_invocation=dataform_v1.WorkflowInvocation(
            compilation_result="test-compilation-result",
        ),
    )

    mock_dataform_client.create_workflow_invocation.assert_called_once_with(
        request=expected_request
    )

    assert workflow_invocation is not None
    assert workflow_invocation.name == "test-workflow-invocation"


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "dev",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_create_workflow_invocation_with_selective_execution(
    mock_dataform_client,
):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    workflow_invocation = resource.create_workflow_invocation(
        compilation_result_name="test-compilation-result",
        included_targets=[
            "target_1",
            {"database": "db", "schema": "schema", "name": "target_2"},
        ],
        included_tags=["tag_1", "tag_2"],
    )

    expected_request = dataform_v1.CreateWorkflowInvocationRequest(
        parent="projects/test-project/locations/us-central1/repositories/test-repo",
        workflow_invocation=dataform_v1.WorkflowInvocation(
            compilation_result="test-compilation-result",
            invocation_config=dataform_v1.InvocationConfig(
                included_targets=[
                    dataform_v1.Target(name="target_1"),
                    dataform_v1.Target(database="db", schema="schema", name="target_2"),
                ],
                included_tags=["tag_1", "tag_2"],
                transitive_dependencies_included=True,
                transitive_dependents_included=False,
                fully_refresh_incremental_tables_enabled=False,
            ),
        ),
    )

    mock_dataform_client.create_workflow_invocation.assert_called_once_with(
        request=expected_request
    )

    assert workflow_invocation is not None
    assert workflow_invocation.name == "test-workflow-invocation"


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "dev",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_get_workflow_invocation_details(
    mock_dataform_client,
):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    workflow_invocation_details = resource.get_workflow_invocation_details(
        workflow_invocation_name="test-workflow-invocation"
    )

    expected_request = dataform_v1.GetWorkflowInvocationRequest(
        name="test-workflow-invocation",
    )

    mock_dataform_client.get_workflow_invocation.assert_called_once_with(
        request=expected_request
    )

    assert workflow_invocation_details is not None
    assert workflow_invocation_details.name == "test-workflow-invocation"
    assert (
        workflow_invocation_details.state
        == dataform_v1.WorkflowInvocation.State.SUCCEEDED
    )


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "dev",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_load_dataform_assets(mock_dataform_client):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    assets = resource.assets

    assert assets is not None
    assert len(assets) == 2
    assert isinstance(assets[0], AssetSpec)
    assert assets[0].kinds == {"bigquery"}
    assert assets[0].metadata["Project ID"] == "test_database"
    assert assets[0].metadata["Dataset"] == "test_schema"
    assert assets[0].metadata["Asset Name"] == "test_asset"


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "dev",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_load_dataform_asset_checks(mock_dataform_client):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    asset_checks = resource.asset_checks

    assert asset_checks is not None
    assert len(asset_checks) == 1
    assert isinstance(asset_checks[0], AssetChecksDefinition)
    assert asset_checks[0].check_specs_by_output_name["spec"].name == "assertion_1"
    assert asset_checks[0].keys_by_input_name["asset_key"].path[0] == "test_asset"


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "dev",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_lazy_load_with_metadata(mock_dataform_client):
    """Test that assets are loaded lazily and have correct metadata when skip_compilation is True."""
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
        skip_compilation=True,
    )

    # Assets should not be loaded initially
    assert resource._assets is None

    # Access assets to trigger lazy load
    assets = resource.assets

    # Verify lazy load occurred
    assert resource._assets is not None
    assert len(assets) == 2

    # Verify metadata contains compilation result name
    assert (
        assets[0].metadata["dataform_compilation_result"] == "test-compilation-result"
    )


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "dev",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_lazy_load_asset_checks(mock_dataform_client):
    """Test that asset checks are loaded lazily when skip_compilation is True."""
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
        skip_compilation=True,
    )

    # Asset checks should not be loaded initially
    assert resource._asset_checks is None

    # Access asset checks to trigger lazy load
    asset_checks = resource.asset_checks

    # Verify lazy load occurred
    assert resource._asset_checks is not None
    assert len(asset_checks) == 1
    assert isinstance(asset_checks[0], AssetChecksDefinition)
    assert asset_checks[0].check_specs_by_output_name["spec"].name == "assertion_1"


@pytest.mark.parametrize(
    "mock_dataform_client",
    [
        {
            "git_commitish": "dev",
            "default_database": "test-database",
            "default_schema": "test-schema",
            "default_location": "us-central1",
            "assertion_schema": "test-assertion-schema",
        }
    ],
    indirect=True,
)
def test_dataform_repository_resource_compile_and_execute_single_target(
    mock_dataform_client,
):
    resource = DataformRepositoryResource(
        project_id="test-project",
        repository_id="test-repo",
        location="us-central1",
        environment="dev",
        client=mock_dataform_client,
    )

    invocation_name = resource.compile_and_execute_single_target(
        target_name="test_target",
        target_project="test-project",
        target_dataset="test_dataset",
        compilation_overrides={"default_database": "test-database"},
    )

    assert invocation_name == "test-workflow-invocation"

    # Verify compilation search
    mock_dataform_client.list_compilation_results.assert_called()

    # Verify workflow invocation creation with correct target
    expected_request = dataform_v1.CreateWorkflowInvocationRequest(
        parent="projects/test-project/locations/us-central1/repositories/test-repo",
        workflow_invocation=dataform_v1.WorkflowInvocation(
            compilation_result="test-compilation-result",
            invocation_config=dataform_v1.InvocationConfig(
                included_targets=[
                    dataform_v1.Target(
                        database="test-project",
                        schema="test_dataset",
                        name="test_target",
                    )
                ],
                transitive_dependencies_included=False,
                fully_refresh_incremental_tables_enabled=False,
            ),
        ),
    )
    mock_dataform_client.create_workflow_invocation.assert_called_with(
        request=expected_request
    )

    # Verify polling
    mock_dataform_client.get_workflow_invocation.assert_called()
