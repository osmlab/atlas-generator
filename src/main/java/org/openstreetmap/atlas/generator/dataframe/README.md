# `AtlasDataFrame`
## Overview
`AtlasDataFrame` is a class that houses methods that convert an atlas RDD to a `DataFrame`. This allows for easily querying the data of an atlas from within an active spark session. Each entity is separated into its own dataframe, and supports its own schema.

### Schemas

Areas:

`identifier` `geometry` `bounds` `tags` `relations`

Edges:

`identifier` `geometry` `start` `end` `inEdges` `outEdges` `hasReverseEdge` `isClosed` `tags` `relations`

Lines:

`identifier` `geometry` `bounds` `isClosed` `tags` `relations`

Nodes:

`identifier` `location` `inEdges` `outEdges` `tags` `relations`

Points:

`identifier` `location` `tags` `relations`

Relations:

`identifier` `geometry` `memberIds` `memberTypes` `memberRoles` `tags` `relations`