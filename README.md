## LGM
Puslar cluser managment app in terminal.
> The signal, a series of sharp pulses that came every 1.3 seconds, seemed too fast to be coming from anything like a star. Bell and Hewish jokingly called the new source LGM-1, for “Little Green Men.”
> [[1]](https://www.aps.org/publications/apsnews/200602/history.cfm)

![image](https://github.com/bloznelis/lgm/assets/33397865/5814d31d-a014-4f05-94c4-acd8326ef101)

### TODO
* Randomize subscription name to avoid collisions
* Check status codes returned from the Pulsar Admin API.
    * For example if it's Unauthorized we should try to parse the body
* Show cluster name
* Read connection info from ~/.config/pulsar/config
* There's a bug while backing out of Listening mode, the cursor disappears.
    * But should rework the cursor handling either way, each resource should have their own cursor.
    That way we can drop "last_*" fields from the App, and always come back to the position we were before in that particular resource.
* Information messages (e.g. "copied to clipboard!")
    * One way to stop showing them would be to assign it a timestamp after which it should disappear.
* To show topic stats, we have to fetch them one-by-one. This seems to be annoying to do in std Rust, so look into Tokio streams.
