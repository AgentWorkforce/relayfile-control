# e2e storm report (oss-cooldown-off)

```json
{
  "flavour": "oss",
  "cooldownEnabled": false,
  "simulatedSeconds": 255,
  "stormSeconds": 120,
  "ingress": {
    "sent": 80,
    "refused429": 35,
    "refusedReasons": [
      "ingest_backpressure:lane_queue_full"
    ],
    "finalAccepted": 80
  },
  "calmLaneDrainedAtSimulatedSeconds": 75,
  "consumeOutcomes": 115,
  "backpressureOutcomes": 30,
  "backpressureOutcomesWithReason": 30,
  "cooldownArmedEvents": 0,
  "cooldownSkipEvents": 0,
  "cooldownSeenArmed": false,
  "cooldownLanesAtEnd": 0,
  "relayfileRequests": 115,
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
