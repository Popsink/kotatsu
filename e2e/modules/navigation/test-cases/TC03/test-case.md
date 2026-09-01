# Test Case: NAV-003

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | NAV-003 |
| **Test Case Title** | Quick-Jump Palette Reaches Topics, Groups and Subjects |
| **Test Type** | Functional, Usability, Accessibility |
| **Priority** | Medium |
| **Estimated Duration** | 3-4 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-09-01 |
| **Last Modified** | 2026-09-01 |

## Test Objective

Verify that the `⌘K` / `Ctrl-K` palette opens from any page, searches topics,
consumer groups and schema subjects in one list, navigates to a chosen result,
and is fully operable from the keyboard alone — including `Esc` to dismiss and a
contained tab order.

## Requirements Traceability

- **User Story**: As a user, I want one keystroke that jumps me to any topic, group or subject so that I do not have to navigate to the right list page first.
- **Requirement ID**: NAV-REQ-003 (Quick-jump palette)
- **Business Rule**: Mounted once in the layout, so it is reachable everywhere. A keystroke is the only thing that starts a request — nothing polls (#101's on-demand contract). Each kind is searched with `search=`/`limit=5`; a section whose match count exceeds the cap offers the full list. Recent selections persist in `localStorage` under `kotatsu:recent` and stand in while the box is empty.

## Preconditions

1. **System State**: Stack up; source connected; schema registry reachable.
2. **Test Data**: topics including `acme.prod.db2.dbz_config` and `avro-orders`; group `qa-group`; subject `avro-orders-value`.
3. **Environment**: Base URL `http://localhost:8080`; browser with a keyboard.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Open from an unrelated page | on `/groups`, press `Ctrl-K` | A dialog named **Quick jump** opens; focus is in the search box |
| 2 | Search across kinds | type `avro-orders` | Sections **Topics** and **Schemas** appear, each with its matches |
| 3 | Verify the first row is active | inspect the rows | The first `option` has `aria-selected="true"`; the box's `aria-activedescendant` names it |
| 4 | Walk with the keyboard | press `↓` twice, then `↑` | The active row moves across section boundaries and wraps at the end |
| 5 | Open with the keyboard | press `Enter` | Navigates to the active result's detail page; the dialog closes |
| 6 | Verify the selection is remembered | reopen the palette with an empty box | A **Recent** section lists the previous selection first |
| 7 | Dismiss | press `Esc` | The dialog closes; the underlying page is unchanged; focus returns where it was |
| 8 | Verify the tab order is contained | reopen, search a term with a capped section, press `Tab` past the last stop | Focus cycles back into the dialog rather than reaching the page behind it |
| 9 | Reach the full list | click **see all** on a capped section | Lands on that list page with the term applied (`/topics?all=1&q=…`) |

## Expected Results

### Primary Verification Points

1. The chord opens the palette from any page, and the same chord closes it.
2. One search returns topics, groups and subjects, grouped by kind.
3. `Enter` on the active row navigates to its detail page.
4. `Esc` closes the palette and returns focus to its origin.

### Secondary Verification Points

5. Recent selections persist across visits and are offered before anything is typed.
6. A capped section says how many matches there are and links to the full list.
7. A kind whose search fails is named, so a short list is not read as complete.
8. A missing schema registry (503) is an absent section, not an error at every keystroke.

## Test Data

```json
{
  "cluster": "demo",
  "term": "avro-orders",
  "sections": [
    { "kind": "topic", "items": ["avro-orders"] },
    { "kind": "subject", "items": ["avro-orders-value"] }
  ],
  "recent_key": "kotatsu:recent"
}
```

## Post-conditions

1. `localStorage` holds the selections made during the run under `kotatsu:recent`.

## Cleanup Steps

1. Clear `kotatsu:recent` if a later case depends on an empty Recent section.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| The chord collides with a browser shortcut | Medium | Low | The handler calls `preventDefault`; step 1 asserts the dialog opens rather than the browser's own search |
| A slow answer overwrites a newer one | Medium | Medium | Each round is sequence-checked and a superseded answer is dropped; covered by unit test |
| Focus escapes the dialog, leaving a keyboard user stranded behind a scrim | Low | High | Step 8 asserts the tab order is contained; `Esc` is always the way out |
| Typing fires a request per keystroke | Medium | Medium | 300 ms debounce, same as the list pages; three requests per settled term |

## Dependencies

- `search=` on `/topics`, `/groups` and `/schemas` (#29–#31) — the palette adds no backend surface.
- Registry reachable for the **Schemas** section; absent registry is covered by secondary point 8.

## Notes

- The palette is also reachable by pointer, from the **Quick jump** button in the sidebar: a keyboard-only affordance is one most users never discover.
- Recent selections are not validated against the cluster, so a topic deleted between visits stays listed until it is pushed out; opening it lands on the detail page's own error state.
