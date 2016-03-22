# EIDAWS-Dataselect-Collector
NodeJS dataselect collector for multiple EIDA nodes routed by GFZ.

## Description
The Collector forwards a dataselect query to the EIDA test implementation of the routing service hosted at GFZ (http://geofon.gfz-potsdam.de/eidaws/routing/1/query).
The routes determined by this query are forwarded to the respective nodes and collected by the Collector service, in the end to be returned to the user.
