Roger Media Service
===================

Handles audio/video tasks such as text-to-speech and transcoding.


Endpoints
---------

### `POST /v1/recognize?file={filename}&bucket={bucket}`

Turns speech into text. The source file should be in Google Storage already.


### `POST /v1/transcode?url={url}&bucket={bucket}`

Transcodes the media file at `{url}` into the Google Storage bucket `{bucket}`.

Additional supported arguments:

* `bitrate`
* `filename` (defaults to `{random}_t.{format}`)
* `sample_rate`
* `format` (defaults to `mp3`)


### `POST /v1/tts?text={text}&bucket={bucket}`

Converts `{text}` to speech and puts it in the Google Storage bucket `{bucket}`.


Pushing a version
-----------------

```bash
./deploy
```
