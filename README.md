## Smalljoin

A small tool, similar to the existing unix `join` tool, but with a focus on:

1. CSV and JSON handling, including splitting CSVS up by column and joining on JSON subfields (ie, for database dumps)
1. Handling an incoming stream of left data as an input, rather than loading it entirely into memory

### Example 

Given a big file such as this:
```
$ cat some-big-file
1,col1,col2,"test","{\"data\": {\"index\":\"a"}}"
2,col1,col2,"test","{\"data\": {\"index\":\"b"}}"
3,col1,col2,"test","{\"data\": {\"index\":\"c"}}"
4,col1,col2,"test","{\"data\": {\"index\":\"d"}}"
...
```

and a small file of the indexes you want to join on
```
$ cat index.csv 
a
b
```

```sh
# stream in the big file,
cat some-big-file | small-join --right index.csv \
    -left-join-column 4 \
    -left-json-subquery  'data.index'
```

gives the result for an 'inner' join by default, showing only those rows from the 'left' (the incoming stream) which match the 'right' (the index file):
```json
{"Left":{"Index":"a","Row":"1,col1,col2,\"test\",\"{\\\"data\\\": {\\\"index\\\":\\\"a\"}}\""},"Right":{"IndexFileResult":{"Index":"a","Row":"a"}}}
{"Left":{"Index":"b","Row":"2,col1,col2,\"test\",\"{\\\"data\\\": {\\\"index\\\":\\\"b\"}}\""},"Right":{"IndexFileResult":{"Index":"b","Row":"b"}}}
```

### Joins

The joining is a simple equality join in go only for now. It relies on the notions of 'left' and 'right'. The 'left' is the incoming data-stream through stdin, the 'right' is the supplied 'file index' file.

Joins supports: 

- "left": ie, take every entry from the incoming stream and see if a match in the joining file can be found 
- 'inner' (default): Only show a result where both the supplied index file and the incoming stream's data can be matched
- 'right-is-null' only show were the incoming stream does *not* have a match in the right index file

### JSON joining support

Both right and left joins can be performed on subfields in the JSON. The query language is standard [JMESpath](https://jmespath.org/). The query needs to reach into the JSON and select a primative (a string, integer or whatever). If this isn't supplied, it'll either join on the entire column or the entire row if `left-join-column/right-join-column` isn't specified.

### Justification and other tools

**Why not use Apache drill/Presto/Flink etc?**

Those are abolutely better, more powerful, more specialised, better maintained and overall superior tools for anything this tiny project can do. However, the reason I wrote this hobby project was:
- They're a sledgehammer and often overkill for a simple problem. Their setup for the kind of work I do: Non-distributed data processing, incident response, scripting and the like didn't require such a powerful toolset
- Such tools are also not particularly fast to get setup with, their overhead of configuration isn't particularly easy and when under time-pressure

** Why not `jq` + `xargs/parallel` + grep?**
- These are battle-tested and great tools, but, without considerable fiddling with subshells, they're not easy to do joins with

** why not use unix `join`?** 
- doesn't support JSON by default as far as I know, making joining on subfields difficult

### Maturity / support
- This is a hobby project and is pre-alpha and doubtlessly is buggy. Open source / MIT licenced as-is etc.

### Other notes
- Output ordering is not guaranteed due to the default behaviour being concurrent in joining

### Building

run `make`

### Installation

For OSX:
```
curl https://github.com/davidporter-id-au/small-join/releases/download/0.0.1/small-join_darwin -o /usr/local/bin/small-join -L \
    && chmod +x /usr/local/bin/small-join
```