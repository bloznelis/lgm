## LGM
Puslar cluser managment app in terminal.
> The signal, a series of sharp pulses that came every 1.3 seconds, seemed too fast to be coming from anything like a star. Bell and Hewish jokingly called the new source LGM-1, for “Little Green Men.”
> [[0]](https://www.aps.org/publications/apsnews/200602/history.cfm)

### TODO
* Show resources as tables with additional info as columns
    * For example consumer count on subscriptions
* Track full resource path
    * Will be clear which tenant/namespace/topic we are in
    * Present it nicely
    * It will allow to backtrack
* More commands
    * Delete subscription
* Some kind of dialog modal for confirmation
* Randomize subscription name to avoid collisions
* Check status codes returned from the Pulsar Admin API.
    * For example if it's Unauthorized we should try to parse the body
* Show cluster name
* Read connection info from ~/.config/pulsar/config
* There's a bug while backing out of Listening mode, the cursor dissapears.
    * But should rework the cursor handling either way, each resource should have their own cursor.
    That way we can drop "last_*" fields from the App, and always come back to the position we were before in that particular resource.
