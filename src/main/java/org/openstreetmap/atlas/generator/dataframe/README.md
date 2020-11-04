# `AtlasDataFrame`
## Overview
`AtlasDataFrame` is a class that houses methods that convert an atlas RDD to a `DataFrame`. This allows for easily querying the data of an atlas from within an active spark session. Each entity is separated into its own dataframe, and supports its own schema.

### Schemas

Areas:

|`identifier` | `geometry` | `bounds` | `tags` | `relations` |
|-----|-----|-----|---|---|
| String | String | String | Map | Array |

Edges:

| `identifier` | `geometry` | `start` | `end` | `inEdges` | `outEdges` | `hasReverseEdge` | `isClosed` | `tags` | `relations` |
|-----|-----|-----|---|---|-----|-----|-----|---|---|
| String | String | String | String | Array | Array | Boolean | Boolean | Map | Array |

Lines:

| `identifier` | `geometry` | `bounds` | `isClosed` | `tags` | `relations` |
|-----|-----|-----|---|---|---|
| String | String | String | String | Boolean | Map | Array |

Nodes:

| `identifier` | `location` | `inEdges` | `outEdges` | `tags` | `relations` |
|-----|-----|-----|---|---|---|
| String | String | Array | Array | Map | Array |

Points:

| `identifier` | `location` | `tags` | `relations` |
|-----|-----|-----|---|
| String | String | Map | Array |

Relations:

| `identifier` | `geometry` | `memberIds` | `memberTypes` | `memberRoles` | `tags` | `relations` |
|-----|-----|-----|---|---|-----|-----|
| String | String | Array | Array | Array | Map | Array |