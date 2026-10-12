---
title: "Consolidating Multiple Apex Triggers into a Single Handler Framework"
description: "How to eliminate order-of-execution race conditions, recursion bugs, and governor limit errors by moving multiple Apex triggers into a unified handler pattern."
pubDate: 2026-10-13
tags: ["Salesforce", "Apex", "Software Engineering", "Architecture"]
---

Salesforce orgs that have been developed over multiple years by different teams or consultants often end up with multiple triggers on the same object:

```apex
// Common antipattern: multiple independent triggers on Account
trigger AccountTerritoryRouting on Account (before insert, before update) { ... }
trigger AccountCreditCheck on Account (after insert, after update) { ... }
trigger AccountEnrichment on Account (before insert) { ... }
trigger AccountAuditLogging on Account (after update) { ... }
```

Salesforce does not guarantee the execution order of multiple triggers on the same object. If trigger execution compiles in one order in a sandbox and a different order in production after a package deployment, business logic that assumes sequential execution can break without warning.

In addition, separate triggers run their own SOQL queries and DML updates independently instead of sharing collections, making it easy to hit governor limits (such as the 101 SOQL query limit) during bulk data operations.

The standard solution is straightforward: keep exactly one trigger per object that does no direct work, and delegate execution to an extensible handler class.

---

## The Issues with Trigger Sprawl

Having multiple triggers on a single object causes four recurring issues:

1. **Unpredictable ordering**: If `AccountTerritoryRouting` expects `AccountEnrichment` to run first and set the billing postal code, an unexpected change in execution order leaves fields blank.
2. **Governor limit pressure**: Each trigger queries and updates records on its own rather than operating over a single bulkified collection.
3. **Uncontrolled recursion**: Trigger A updates related Contacts, which fires a Contact trigger that updates the Account parent, re-firing Trigger B until hitting the maximum stack depth of 16.
4. **Testing friction**: Triggers with embedded business logic cannot be easily bypassed or unit-tested in isolation without executing the full database pipeline.

---

## The Base `TriggerHandler` Class

To enforce execution order and provide hooks for bypassing logic during migrations and tests, we use a lightweight abstract handler base class:

```apex
public virtual class TriggerHandler {
    // Allows bypassing specific handlers during test setup or bulk migrations
    private static Set<String> bypassedHandlers = new Set<String>();

    // Tracks execution depth to prevent infinite recursive loops
    private static Map<String, Integer> loopCounts = new Map<String, Integer>();
    private Integer maxLoopCount = 2;

    public void run() {
        if (!Trigger.isExecuting) {
            throw new TriggerHandlerException('TriggerHandler must only be called from trigger context.');
        }

        String handlerName = getHandlerName();
        if (bypassedHandlers.contains(handlerName)) {
            return;
        }

        trackLoopCount(handlerName);

        switch on Trigger.operationType {
            when BEFORE_INSERT {
                beforeInsert();
            }
            when BEFORE_UPDATE {
                beforeUpdate();
            }
            when BEFORE_DELETE {
                beforeDelete();
            }
            when AFTER_INSERT {
                afterInsert();
            }
            when AFTER_UPDATE {
                afterUpdate();
            }
            when AFTER_DELETE {
                afterDelete();
            }
            when AFTER_UNDELETE {
                afterUndelete();
            }
        }
    }

    // Context hooks overridden in child handlers
    protected virtual void beforeInsert() {}
    protected virtual void beforeUpdate() {}
    protected virtual void beforeDelete() {}
    protected virtual void afterInsert() {}
    protected virtual void afterUpdate() {}
    protected virtual void afterDelete() {}
    protected virtual void afterUndelete() {}

    public static void bypass(String handlerName) {
        bypassedHandlers.add(handlerName);
    }

    public static void clearBypass(String handlerName) {
        bypassedHandlers.remove(handlerName);
    }

    public static Boolean isBypassed(String handlerName) {
        return bypassedHandlers.contains(handlerName);
    }

    private void trackLoopCount(String handlerName) {
        Integer currentCount = loopCounts.containsKey(handlerName) ? loopCounts.get(handlerName) : 0;
        currentCount++;
        loopCounts.put(handlerName, currentCount);

        if (currentCount > this.maxLoopCount) {
            throw new TriggerHandlerException(
                'Maximum loop count of ' + this.maxLoopCount + ' exceeded for ' + handlerName
            );
        }
    }

    private String getHandlerName() {
        return String.valueOf(this).substring(0, String.valueOf(this).indexOf(':'));
    }

    public class TriggerHandlerException extends Exception {}
}
```

---

## Delegating from the Trigger

With the base handler in place, the trigger itself is reduced to a single line of execution:

```apex
trigger AccountTrigger on Account (
    before insert, before update, before delete,
    after insert, after update, after delete, after undelete
) {
    new AccountTriggerHandler().run();
}
```

The concrete class `AccountTriggerHandler` orchestrates the logic, calling dedicated service methods in explicit sequence:

```apex
public with sharing class AccountTriggerHandler extends TriggerHandler {
    private List<Account> newAccounts;
    private Map<Id, Account> oldAccountMap;
    private Map<Id, Account> newAccountMap;

    public AccountTriggerHandler() {
        this.newAccounts = (List<Account>) Trigger.new;
        this.oldAccountMap = (Map<Id, Account>) Trigger.oldMap;
        this.newAccountMap = (Map<Id, Account>) Trigger.newMap;
    }

    public override void beforeInsert() {
        // Step 1: Standardize addresses
        AccountDataEnrichmentService.standardizeContactInfo(this.newAccounts);

        // Step 2: Route territory based on standardized data
        AccountRoutingService.assignTerritoryCodes(this.newAccounts);
    }

    public override void beforeUpdate() {
        // Only evaluate records where relevant fields actually changed
        List<Account> addressChanged = new List<Account>();
        for (Account acc : this.newAccounts) {
            Account oldAcc = this.oldAccountMap.get(acc.Id);
            if (acc.BillingPostalCode != oldAcc.BillingPostalCode) {
                addressChanged.add(acc);
            }
        }

        if (!addressChanged.isEmpty()) {
            AccountRoutingService.assignTerritoryCodes(addressChanged);
        }
    }

    public override void afterUpdate() {
        AccountSyncService.publishAccountUpdatedEvents(this.newAccounts, this.oldAccountMap);
    }
}
```

This pattern has two immediate practical benefits:
- **Explicit execution order**: Address standardization is guaranteed to finish before territory assignment runs.
- **In-memory change filtering**: Filtering records on fields that actually changed before calling services avoids unnecessary queries and calculations.

---

## Deploying the Changes Safely

When migrating legacy triggers in production, deactivating old triggers and deploying the new consolidated handler must happen in the same changeset:

1. **Inventory existing triggers**: Use the Tooling API to query all active triggers on the object:
   ```sql
   SELECT Id, Name, TableEnumOrId, Status
   FROM ApexTrigger
   WHERE Status = 'Active' AND TableEnumOrId = 'Account'
   ```
2. **Move business logic into service classes**: Keep the handler lean and move calculations, callouts, and queries into reusable static or instance service classes.
3. **Deactivate and deploy simultaneously**: Mark old triggers as `Inactive` in `destructiveChangesPre.xml` and deploy the new handler class and unified trigger in the same metadata deployment package.
4. **Use bypasses in unit tests**: For unit tests on peripheral objects (for example, creating mock Accounts inside an Opportunity test), bypass the Account handler to reduce test execution times across CI builds:
   ```apex
   @IsTest
   private class OpportunityServiceTest {
       @IsTest
       static void testOpportunityCalculation() {
           TriggerHandler.bypass('AccountTriggerHandler');

           Account acc = new Account(Name = 'Test Enterprise');
           insert acc;

           TriggerHandler.clearBypass('AccountTriggerHandler');

           // Run opportunity-specific test logic...
       }
   }
   ```

---

## Conclusion

Consolidating multiple triggers into a single handler per object provides deterministic ordering, prevents recursive loops, and makes bulk operations much less likely to hit governor limits.
