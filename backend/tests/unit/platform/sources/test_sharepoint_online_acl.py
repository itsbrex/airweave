"""Unit tests for SharePoint Online ACL extraction.

Covers ``extract_access_control`` and the rules around how Microsoft Graph
sharing-link permissions map to ``AccessControl.is_public``.

Background — bug fix:
    Organization-scoped sharing links (``link.scope == "organization"``,
    "anyone in your org with the link") used to set ``is_public = True``,
    which made the search broker bypass all viewer checks. That mis-modeled
    SharePoint semantics: an org-scoped link requires possession of the
    link URL to grant access. Only ``link.scope == "anonymous"`` is true
    public access.
"""

import pytest

from airweave.platform.sources.sharepoint_online.acl import extract_access_control

# ---------------------------------------------------------------------------
# Helpers — build minimal Graph permission objects
# ---------------------------------------------------------------------------


def _link_perm(scope: str, roles=None) -> dict:
    """Sharing-link permission with the given scope (no grantedTo principal)."""
    return {
        "id": f"link-{scope}",
        "roles": roles if roles is not None else ["write"],
        "link": {"scope": scope, "type": "edit"},
        "grantedToIdentitiesV2": [],
        "grantedToIdentities": [],
    }


def _site_group_perm(name: str, group_id: str = "5", roles=None) -> dict:
    return {
        "id": f"sg-{group_id}",
        "roles": roles if roles is not None else ["write"],
        "grantedToV2": {"siteGroup": {"displayName": name, "id": group_id}},
    }


def _user_perm(email: str, roles=None) -> dict:
    return {
        "id": f"u-{email}",
        "roles": roles if roles is not None else ["read"],
        "grantedToV2": {"user": {"email": email, "displayName": email}},
    }


def _entra_group_perm(group_id: str, roles=None) -> dict:
    return {
        "id": f"eg-{group_id}",
        "roles": roles if roles is not None else ["read"],
        "grantedToV2": {"group": {"id": group_id}},
    }


# ---------------------------------------------------------------------------
# Sharing-link scope handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_organization_scoped_link_does_not_set_is_public():
    """Org-scoped link by itself must not flip is_public.

    Regression: the previous behavior treated organization-scoped links as
    fully public, bypassing all viewer checks at search time.
    """
    ac = await extract_access_control([_link_perm("organization")])
    assert ac.is_public is False
    assert ac.viewers == []


@pytest.mark.asyncio
async def test_anonymous_link_sets_is_public():
    ac = await extract_access_control([_link_perm("anonymous")])
    assert ac.is_public is True


@pytest.mark.asyncio
async def test_org_and_anonymous_links_together_still_public_via_anonymous():
    ac = await extract_access_control([_link_perm("organization"), _link_perm("anonymous")])
    assert ac.is_public is True


@pytest.mark.asyncio
async def test_users_scoped_link_does_not_set_is_public():
    """``users``-scoped links target named recipients, not the org."""
    ac = await extract_access_control([_link_perm("users")])
    assert ac.is_public is False


@pytest.mark.asyncio
async def test_unknown_link_scope_does_not_set_is_public():
    """Future / unrecognized scopes default to non-public."""
    ac = await extract_access_control([_link_perm("someFutureScope")])
    assert ac.is_public is False


# ---------------------------------------------------------------------------
# Mixed permissions — the realistic case
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_org_link_alongside_explicit_grants_extracts_only_grants():
    """Org-link permission is skipped; explicit grants populate viewers.

    Mirrors the Mistral bug-report payload: a file with one organization-
    scoped sharing link plus the inherited site-group grants. The fix must
    keep is_public false while still extracting Owners / Members / Visitors.
    """
    perms = [
        _link_perm("organization"),
        _site_group_perm("Access Control Tests Owners", group_id="3", roles=["owner"]),
        _site_group_perm("Access Control Tests Members", group_id="5", roles=["write"]),
        _site_group_perm("Access Control Tests Visitors", group_id="4", roles=["read"]),
    ]
    ac = await extract_access_control(perms)
    assert ac.is_public is False
    assert set(ac.viewers) == {
        "group:sp:access_control_tests_owners",
        "group:sp:access_control_tests_members",
        "group:sp:access_control_tests_visitors",
    }


@pytest.mark.asyncio
async def test_user_and_entra_group_grants_extracted():
    perms = [
        _user_perm("alice@example.com"),
        _entra_group_perm("11111111-2222-3333-4444-555555555555"),
    ]
    ac = await extract_access_control(perms)
    assert ac.is_public is False
    assert set(ac.viewers) == {
        "user:alice@example.com",
        "group:entra:11111111-2222-3333-4444-555555555555",
    }


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_permissions_returns_empty_access_control():
    ac = await extract_access_control([])
    assert ac.is_public is False
    assert ac.viewers == []


@pytest.mark.asyncio
async def test_permission_without_read_role_is_ignored():
    """Roles other than read/write/owner/sp.full control don't grant viewing."""
    perms = [
        {
            "id": "restricted",
            "roles": ["restricted"],
            "grantedToV2": {"user": {"email": "alice@example.com"}},
        },
    ]
    ac = await extract_access_control(perms)
    assert ac.is_public is False
    assert ac.viewers == []


@pytest.mark.asyncio
async def test_org_link_without_read_role_is_ignored_entirely():
    """A link without a read-equivalent role doesn't even reach scope check."""
    perms = [
        {
            "id": "link-restricted",
            "roles": ["restricted"],
            "link": {"scope": "organization"},
        }
    ]
    ac = await extract_access_control(perms)
    assert ac.is_public is False
    assert ac.viewers == []


@pytest.mark.asyncio
async def test_duplicate_principal_only_added_once():
    perms = [
        _user_perm("alice@example.com", roles=["read"]),
        _user_perm("alice@example.com", roles=["write"]),
    ]
    ac = await extract_access_control(perms)
    assert ac.viewers == ["user:alice@example.com"]
