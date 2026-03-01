import pytest

from helper_func import comparison_models
from receiving_fixtures import create_requirement, create_processing
from src.schemas.response import UserOut, ResumeOut, RequirementsOut, ProcessingOut


@pytest.mark.asyncio
async def test_health_check(
    client_with_db
):
    response = await client_with_db.get("storage/health")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_me(client_with_db, create_user):
    user, accesses_token = await create_user()
    response = await client_with_db.get("storage/me", headers={"Authorization": f"Bearer {accesses_token}"})

    assert response.status_code == 200

    user_out = UserOut(**(response.json()))

    assert user_out.user_id == user.user_id
    assert user_out.username == user.username


@pytest.mark.asyncio
async def test_get_resume(resume_service_fix, create_resume, create_user, client_with_db):
    user, accesses_token = await create_user()
    new_resume = await create_resume(user.user_id)

    response = await client_with_db.get(
        "storage/get_resume",
        headers={"Authorization": f"Bearer {accesses_token}"},
        params = {'resume_id': new_resume.resume_id},
    )

    assert response.status_code == 200

    resume_out = ResumeOut(**(response.json()))

    assert new_resume.resume_id == resume_out.resume_id
    assert new_resume.resume == resume_out.resume


@pytest.mark.parametrize(
    'use_redis',
    [True,False,]
)
@pytest.mark.asyncio
async def test_get_resume_by_requirement(
    use_redis,
    create_user,
    resume_service_fix,
    create_resume,
    create_requirement,
    client_with_db
):
    user, accesses_token = await create_user()
    requirement = await create_requirement(user.user_id)
    resume_1 = await create_resume(user_id=user.user_id, requirement_id=requirement.requirement_id)
    resume_2 = await create_resume(user_id=user.user_id, requirement_id=requirement.requirement_id)
    resume_other = await create_resume() # резюме у другого пользователя

    if not use_redis:
        await resume_service_fix.resume_cache_repo.delete_by_requirement(requirement.requirement_id)

    response = await client_with_db.get(
        "storage/get_resume_by_requirement",
        headers={"Authorization": f"Bearer {accesses_token}"},
        params = {'requirement_id': requirement.requirement_id},
    )

    assert response.status_code == 200

    list_resume_dicts = response.json()
    assert [ResumeOut(**resume_dict) for resume_dict in list_resume_dicts] # просто должен нормально преобразоваться

    assert any(comparison_models(resume_1.to_dict(), resume_dict) for resume_dict in list_resume_dicts)
    assert any(comparison_models(resume_2.to_dict(), resume_dict) for resume_dict in list_resume_dicts)
    assert not any(comparison_models(resume_other.to_dict(), resume_dict) for resume_dict in list_resume_dicts)

    if not use_redis:
        # в redis должны обновить данные
        resumes_in_redis = await resume_service_fix.resume_cache_repo.get_by_requirement(requirement.requirement_id)
        assert any(comparison_models(resume_1, resume) for resume in resumes_in_redis)
        assert any(comparison_models(resume_2, resume) for resume in resumes_in_redis)
        assert not any(comparison_models(resume_other, resume) for resume in resumes_in_redis)


@pytest.mark.asyncio
async def test_get_requirements(create_user, client_with_db, create_requirement):
    user, accesses_token = await create_user()
    new_requirement = await create_requirement(user.user_id)

    response = await client_with_db.get(
        f"storage/get_requirement/{new_requirement.requirement_id}",
        headers={"Authorization": f"Bearer {accesses_token}"},
    )

    assert response.status_code == 200

    requirement_out = RequirementsOut(**(response.json()))

    assert comparison_models(new_requirement, requirement_out.model_dump())


@pytest.mark.parametrize(
    'use_redis',
    [True,False,]
)
@pytest.mark.asyncio
async def test_get_requirements(
    use_redis,
    create_user,
    create_requirement,
    requirement_service_fix,
    client_with_db
):
    user, accesses_token = await create_user()
    requirement_1 = await create_requirement(user.user_id)
    requirement_2 = await create_requirement(user.user_id)
    requirement_other = await create_requirement() # требования у другого пользователя

    if not use_redis:
        await requirement_service_fix.requirement_cache_repo.delete_by_user(user.user_id)

    response = await client_with_db.get(
        "storage/get_requirements",
        headers={"Authorization": f"Bearer {accesses_token}"},
    )

    assert response.status_code == 200

    list_requirement_dicts = response.json()
    assert [RequirementsOut(**resume_dict) for resume_dict in list_requirement_dicts] # просто должен нормально преобразоваться

    assert any(comparison_models(requirement_1.to_dict(), resume_dict) for resume_dict in list_requirement_dicts)
    assert any(comparison_models(requirement_2.to_dict(), resume_dict) for resume_dict in list_requirement_dicts)
    assert not any(comparison_models(requirement_other.to_dict(), resume_dict) for resume_dict in list_requirement_dicts)

    if not use_redis:
        # в redis должны обновить данные
        resumes_in_redis = await requirement_service_fix.requirement_cache_repo.get_by_user(user.user_id)
        assert any(comparison_models(requirement_1, resume) for resume in resumes_in_redis)
        assert any(comparison_models(requirement_2, resume) for resume in resumes_in_redis)
        assert not any(comparison_models(requirement_other, resume) for resume in resumes_in_redis)


@pytest.mark.asyncio
async def test_get_processing_by_resume(create_user, client_with_db, create_resume, create_processing):
    user, accesses_token = await create_user()
    processing = await create_processing(user_id=user.user_id)

    response = await client_with_db.get(
        f"storage/get_processing_by_resume/{processing.resume_id}",
        headers={"Authorization": f"Bearer {accesses_token}"},
    )

    assert response.status_code == 200

    processing_out = ProcessingOut(**(response.json()))

    assert comparison_models(processing, processing_out.model_dump())


@pytest.mark.asyncio
async def test_check_exceptions(create_user, client_with_db):
    user, accesses_token = await create_user()

    response = await client_with_db.get(
        f"storage/get_processing_by_resume/1",
        headers={"Authorization": f"Bearer {accesses_token}"},
    )

    # не должно происходить исключение внутри сервера, а должен пройти через exception_handler
    assert response.status_code == 404

    response = await client_with_db.get(
        f"storage/get_processing_by_resume/1",
    )
    assert response.status_code == 401
