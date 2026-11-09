# e2e storm report (oss-cooldown-on)

```json
{
  "flavour": "oss",
  "cooldownEnabled": true,
  "simulatedSeconds": 195,
  "stormSeconds": 120,
  "ingress": {
    "sent": 80,
    "refused429": 35,
    "refusedReasons": [
      "ingest_backpressure:lane_queue_full"
    ],
    "finalAccepted": 80
  },
  "calmLaneDrainedAtSimulatedSeconds": 45,
  "consumeOutcomes": 88,
  "backpressureOutcomes": 6,
  "backpressureOutcomesWithReason": 6,
  "cooldownArmedEvents": 6,
  "cooldownSkipEvents": 9,
  "cooldownSeenArmed": true,
  "cooldownLanesAtEnd": 0,
  "relayfileRequests": 88,
  "relayfileStored": 80,
  "relayfileRejectedSignatures": 0,
  "unwrittenAcks": 0,
  "dead": 0,
  "dlqArchived": 0,
  "finalStates": [
    {
      "lane": "rw_storm",
      "state": "done",
      "n": 60
    },
    {
      "lane": "rw_calm",
      "state": "done",
      "n": 20
    }
  ]
}
```
