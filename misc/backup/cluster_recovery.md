# Cluster Recovery Procedure -- Redis Active/Active

## 1. Purpose

This document simulates a Redis cluster shutdown scenario and describes
the steps to recover an **Active/Active Redis database** after
reinstalling a cluster.

The test scenario assumes:

-   **Cluster A** → will be shut down and reinstalled\
-   **Cluster B** → remains up and running

The goal is to verify that the Active/Active database is restored and
synchronized correctly after Cluster A is recovered.

------------------------------------------------------------------------

## 2. Pre-requisites

-   Access to OpenShift clusters (Cluster A and Cluster B)
-   Access to Redis Enterprise UI
-   Access to Ansible automation scripts
-   Required YAML files for RERC objects (`rerc-1.yaml`, `rerc-2.yaml`)
-   Appropriate permissions to modify REAADB and Redis objects

------------------------------------------------------------------------

## 3. Execution Steps

### Step 1: Remove Cluster A from REAADB (on Cluster B side)

1.  Edit the REAADB resource:

    ``` bash
    oc edit reaadb
    ```

2.  Locate the following field:

        spec.participatingClusters

3.  Remove **Cluster A** from the participatingClusters list.

4.  Save and exit the file.

------------------------------------------------------------------------

### Step 2: Verify Cluster A removal

1.  Wait until Cluster A is removed from the REAADB
    participatingClusters list.
2.  Confirm that Cluster B remains active and stable.

------------------------------------------------------------------------

### Step 3: Delete remaining Redis Enterprise DB (REDB) objects on Cluster A

1.  Delete remaining REDB objects associated with Cluster A.

------------------------------------------------------------------------

### Step 4: Delete Redis Enterprise Remote Cluster (RERC) objects

On Cluster A, delete both RERC objects:

``` bash
oc delete -f rerc-1.yaml
oc delete -f rerc-2.yaml
```

------------------------------------------------------------------------

### Step 5: Delete Redis Enterprise Cluster (REC)

1.  Delete the Redis Enterprise Cluster (REC) on Cluster A.
2.  This will remove Redis from Cluster A completely.

------------------------------------------------------------------------

### Step 6: Verify Cluster A is down

1.  Confirm that Cluster A is fully shut down.
2.  Ensure no Redis pods or related resources are running.

------------------------------------------------------------------------

### Step 7: Reinstall Cluster A using Ansible

1.  Navigate to the Ansible scripts folder.

2.  Run the Ansible one-shot script:

    ``` bash
    ansible-playbook <oneshot-script>.yaml
    ```

3.  Wait for the Ansible execution to complete successfully.

------------------------------------------------------------------------

### Step 8: Verify Cluster Recovery

1.  Log in to the OpenShift console for Cluster A.
2.  Verify that all Redis pods are running successfully.
3.  Open the Redis Enterprise UI.
4.  Confirm that:
    -   The Active/Active database is visible.
    -   Data is synchronized between Cluster A and Cluster B.

------------------------------------------------------------------------

## 4. Expected Result

-   Cluster A is successfully reinstalled.
-   Active/Active Redis database is restored.
-   Data synchronization between Cluster A and Cluster B is verified.
-   No data loss or replication issues are observed.
