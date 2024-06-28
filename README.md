# LGM
Puslar cluser managment app in terminal.
> The signal, a series of sharp pulses that came every 1.3 seconds, seemed too fast to be coming from anything like a star. Bell and Hewish jokingly called the new source LGM-1, for “Little Green Men.”
>  [[1]](https://www.aps.org/publications/apsnews/200602/history.cfm)

![image](https://github.com/bloznelis/lgm/assets/33397865/5814d31d-a014-4f05-94c4-acd8326ef101)

## Install
### Cargo
`cargo install lgm`

**note:** you might need some system-wide libraries, like protobuf and libx11-dev for `lgm` to build.

### Prebuilt binaries
Head over to the [releases](https://github.com/bloznelis/lgm/releases) and grab the latest binary based on your platform.

### TODO
* Token auth for subscriptions
* To show topic stats, we have to fetch them one-by-one. This seems to be annoying to do in std Rust, so look into Tokio streams.
* Auto refresh
