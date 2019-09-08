package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
)

const (
	ConfigPath = "./config.json"
)

type Config struct {
	ListenAddr string
	Buckets    []string
	Watson     struct{ Username, Password string }
}

type recognizeAlternative struct {
	Transcript string  `json:"transcript"`
	Confidence float64 `json:"confidence"`
}

type recognizeError struct {
	Code    int                      `json:"code"`
	Message string                   `json:"message"`
	Details []map[string]interface{} `json:"details"`
}

func (e *recognizeError) Error() string {
	return e.Message
}

type recognizeRequest struct {
	Config recognizeRequestConfig `json:"config"`
	Audio  recognizeRequestAudio  `json:"audio"`
}

type recognizeRequestConfig struct {
	Encoding        string                   `json:"encoding,omitempty"`
	LanguageCode    string                   `json:"languageCode,omitempty"`
	MaxAlternatives int                      `json:"maxAlternatives,omitempty"`
	ProfanityFilter bool                     `json:"profanityFilter,omitempty"`
	SampleRate      int                      `json:"sampleRate,omitempty"`
	SpeechContext   *recognizeRequestContext `json:"speechContext,omitempty"`
}

type recognizeRequestAudio struct {
	Content string `json:"content,omitempty"`
	URI     string `json:"uri,omitempty"`
}

type recognizeRequestContext struct {
	Phrases []string `json:"phrases"`
}

type recognizeResponse struct {
	Error   *recognizeError   `json:"error"`
	Results []recognizeResult `json:"results"`
}

type recognizeResult struct {
	Alternatives []recognizeAlternative `json:"alternatives"`
}

type transcodeResult struct {
	BucketPath string `json:"bucket_path"`
	Duration   int64  `json:"duration"`
}

type ttsRequest struct {
	Text string `json:"text"`
}

var (
	config     Config
	ctx        = context.Background()
	client     *storage.Client
	gclient    *http.Client
	buckets    map[string]*storage.BucketHandle
	reDuration = regexp.MustCompile(` time=(\d+):(\d+):(\d+)\.(\d+) `)
)

var (
	ErrNotFound = errors.New("could not find the specified resource")
)

func main() {
	// Load configuration from file.
	data, err := ioutil.ReadFile(ConfigPath)
	if err != nil {
		log.Fatalf("Failed to load config (ioutil.ReadFile: %v)", err)
	}
	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("Failed to load config (json.Unmarshal: %v)", err)
	}

	gclient, err = google.DefaultClient(ctx, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		log.Fatalf("Failed to create HTTP client (google.DefaultClient: %v)", err)
	}

	// Set up the Storage client.
	client, err = storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create Storage client (storage.NewClient: %v)", err)
	}

	// Create bucket references.
	buckets = make(map[string]*storage.BucketHandle)
	for _, name := range config.Buckets {
		buckets[name] = client.Bucket(name)
	}

	// Set up server for handling incoming requests.
	http.HandleFunc("/v1/recognize", recognizeHandler)
	http.HandleFunc("/v1/thumbnail", thumbnailHandler)
	http.HandleFunc("/v1/transcode", transcodeHandler)
	http.HandleFunc("/v1/tts", ttsHandler)

	log.Printf("Starting server on %s...", config.ListenAddr)
	if err := http.ListenAndServe(config.ListenAddr, nil); err != nil {
		log.Fatalf("Failed to serve (http.ListenAndServe: %v)", err)
	}
}

// Request handlers.

func recognizeHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	bucketName := q.Get("bucket")
	filename := q.Get("filename")
	beforeTranscode := time.Now()
	if ext := filepath.Ext(filename); ext != ".wav" {
		newFilename := filename[:len(filename)-len(ext)] + ".wav"
		bucket, ok := buckets[bucketName]
		if !ok {
			log.Printf("Invalid bucket %s", bucketName)
			w.WriteHeader(400)
			return
		}
		src, err := bucket.Object(filename).NewReader(ctx)
		if err != nil {
			log.Printf("Failed to transcode %s: %v", filename, err)
			w.WriteHeader(400)
			return
		}
		dst := bucket.Object(newFilename).NewWriter(ctx)
		transcodeStream(dst, src, "-ac", "1", "-ar", "16000", "-f", "s16le", "pipe:1")
		err = dst.Close()
		if err != nil {
			log.Printf("Failed to transcode %s: %v", filename, err)
			w.WriteHeader(400)
			return
		}
		filename = newFilename
	}
	beforeRecognize := time.Now()
	result, err := recognize(filename, bucketName, q.Get("language"))
	if err != nil {
		log.Printf("Failed to recognize %s: %v", filename, err)
		w.WriteHeader(400)
		return
	}
	data, err := json.Marshal(result)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
		w.WriteHeader(500)
		return
	}
	log.Printf("/v1/recognize [ffmpeg: %s, recognize: %s]", beforeRecognize.Sub(beforeTranscode), time.Since(beforeRecognize))
	w.Write(data)
}

func thumbnailHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	// Transcode URL specified in the query string.
	input, err := url.Parse(q.Get("url"))
	if err != nil {
		log.Printf("Failed to get URL argument: %v", err)
		w.WriteHeader(400)
		return
	}
	// Set up the ffmpeg command line arguments.
	arg := []string{
		"-vf", "thumbnail",
		"-frames:v", "1",
		"-pix_fmt", "yuvj420p",
	}
	beforeTranscode := time.Now()
	result, err := transcodeToBucket(q.Get("bucket"), q.Get("filename"), "mjpeg", input, arg...)
	if err != nil {
		log.Printf("Failed to create thumbnail for %v: %v", input, err)
		if err == ErrNotFound {
			w.WriteHeader(404)
		} else {
			w.WriteHeader(400)
		}
		return
	}
	data, err := json.Marshal(result)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
		w.WriteHeader(500)
		return
	}
	log.Printf("/v1/thumbnail [ffmpeg: %s]", time.Since(beforeTranscode))
	w.Write(data)
}

func transcodeHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	// The input can either be a URL string, or an io.Reader.
	var input interface{}
	// Check if there's a file upload in the request.
	f, _, err := r.FormFile("data")
	if err == nil {
		defer f.Close()
		input = f
	} else {
		// Transcode URL specified in the query string.
		input, err = url.Parse(q.Get("url"))
		if err != nil {
			log.Printf("Failed to get URL argument: %v", err)
			w.WriteHeader(400)
			return
		}
	}
	// Set up the ffmpeg command line arguments.
	arg := []string{}
	format := q.Get("format")
	if format == "" {
		format = "mp3"
	}
	if bitrate := q.Get("bitrate"); bitrate != "" {
		arg = append(arg, "-b:a", bitrate)
	}
	if sampleRate := q.Get("sample_rate"); sampleRate != "" {
		arg = append(arg, "-ar", sampleRate)
	}
	beforeTranscode := time.Now()
	result, err := transcodeToBucket(q.Get("bucket"), q.Get("filename"), format, input, arg...)
	if err != nil {
		log.Printf("Failed to transcode %v: %v", input, err)
		if err == ErrNotFound {
			w.WriteHeader(404)
		} else {
			w.WriteHeader(400)
		}
		return
	}
	data, err := json.Marshal(result)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
		w.WriteHeader(500)
		return
	}
	log.Printf("/v1/transcode [ffmpeg: %s]", time.Since(beforeTranscode))
	w.Write(data)
}

func ttsHandler(w http.ResponseWriter, r *http.Request) {
	a := time.Now()
	q := r.URL.Query()
	var text string
	if text = q.Get("text"); text == "" {
		log.Printf("Missing text parameter")
		w.WriteHeader(400)
		return
	}
	audio, err := textToSpeech(text, q.Get("voice"))
	if err != nil {
		log.Printf("Text-to-speech request failed: %v", err)
		w.WriteHeader(503)
		return
	}
	defer audio.Close()
	b := time.Now()
	result, err := transcodeToBucket(q.Get("bucket"), "", "mp3", audio)
	if err != nil {
		log.Printf("Failed to transcode TTS: %v", err)
		w.WriteHeader(400)
		return
	}
	data, err := json.Marshal(result)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
		w.WriteHeader(500)
		return
	}
	log.Printf("/v1/tts [watson: %s] [ffmpeg: %s]", b.Sub(a), time.Since(b))
	w.Write(data)
}

// Utility functions.

func asyncCopy(dst io.Writer, src io.Reader) <-chan error {
	ch := make(chan error)
	go func() {
		_, err := io.Copy(dst, src)
		ch <- err
	}()
	return ch
}

func durations(in [][]byte) (values []time.Duration) {
	values = make([]time.Duration, len(in))
	for i, b := range in {
		d, _ := strconv.Atoi(string(b))
		values[i] = time.Duration(d)
	}
	return
}

func recognize(filename, bucket, language string) (*recognizeAlternative, error) {
	input := recognizeRequest{
		Config: recognizeRequestConfig{
			Encoding:     "LINEAR16",
			LanguageCode: language,
			SampleRate:   16000,
		},
		Audio: recognizeRequestAudio{
			URI: fmt.Sprintf("gs://%s/%s", bucket, filename),
		},
	}
	data, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", "https://speech.googleapis.com/v1beta1/speech:syncrecognize", bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	resp, err := gclient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var body struct {
		Responses []recognizeResponse `json:"responses"`
	}
	err = json.Unmarshal(data, &body)
	if err != nil {
		return nil, err
	}
	if len(body.Responses) == 0 {
		return nil, fmt.Errorf("got an empty response body from speech:syncrecognize")
	}
	if body.Responses[0].Error != nil {
		return nil, body.Responses[0].Error
	}
	if len(body.Responses[0].Results) == 0 || len(body.Responses[0].Results[0].Alternatives) == 0 {
		return nil, fmt.Errorf("got 0 results from speech:syncrecognize")
	}
	return &body.Responses[0].Results[0].Alternatives[0], nil
}

func runCommand(dst io.Writer, src io.Reader, name string, arg ...string) (output []byte, err error) {
	cmd := exec.Command(name, arg...)
	// Set up all the pipes.
	var stdin io.WriteCloser
	if src != nil {
		stdin, err = cmd.StdinPipe()
		if err != nil {
			return
		}
	}
	var stdout io.ReadCloser
	if dst != nil {
		stdout, err = cmd.StdoutPipe()
		if err != nil {
			return
		}
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return
	}
	// Run command.
	err = cmd.Start()
	if err != nil {
		return
	}
	if dst != nil && src != nil {
		// Concurrently read source into stdin and stdout into destination.
		ch1 := asyncCopy(stdin, src)
		ch2 := asyncCopy(dst, stdout)
		err = <-ch1
		if err != nil {
			// TODO: Allow stderr to be read.
			return
		}
		stdin.Close()
		err = <-ch2
	} else if dst != nil {
		// Copy stdout to the destination writer.
		_, err = io.Copy(dst, stdout)
	} else if src != nil {
		// Copy the source reader to stdin.
		_, err = io.Copy(stdin, src)
		stdin.Close()
	}
	if err != nil {
		// TODO: Allow stderr to be read.
		return
	}
	t := time.AfterFunc(time.Minute, func() {
		err := cmd.Process.Kill()
		if err != nil {
			log.Printf("WARNING: Failed to kill command: %v", err)
		}
	})
	// Read the command's output information.
	output, err = ioutil.ReadAll(stderr)
	err = cmd.Wait()
	if !t.Stop() {
		log.Println("WARNING: command took too long")
	}
	return
}

func runFfmpegCommand(dst io.Writer, src io.Reader, arg ...string) (duration time.Duration, err error) {
	output, err := runCommand(dst, src, "ffmpeg", arg...)
	if err != nil {
		// TODO: Consider making this more bullet proof.
		if bytes.Contains(output, []byte("HTTP error 404 Not Found")) {
			err = ErrNotFound
		} else if len(output) > 0 {
			log.Println(string(output))
		}
		return
	}
	matches := reDuration.FindAllSubmatch(output, -1)
	if len(matches) == 0 {
		log.Println(string(output))
		err = fmt.Errorf("failed to parse duration from ffmpeg")
		return
	}
	// Use the four capturing groups from the LAST match.
	d := durations(matches[len(matches)-1][1:])
	duration = d[0]*time.Hour + d[1]*time.Minute + d[2]*time.Second + d[3]*time.Second/100
	return
}

func textToSpeech(text, voice string) (io.ReadCloser, error) {
	data, err := json.Marshal(ttsRequest{Text: text})
	if err != nil {
		return nil, err
	}
	u, err := url.Parse("https://stream.watsonplatform.net/text-to-speech/api/v1/synthesize")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	if voice != "" {
		q.Set("voice", voice)
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequest("POST", u.String(), bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(config.Watson.Username, config.Watson.Password)
	req.Header.Set("Accept", "audio/flac")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func transcodeStream(dst io.Writer, src io.Reader, arg ...string) (time.Duration, error) {
	arg = append([]string{"-i", "pipe:0"}, arg...)
	return runFfmpegCommand(dst, src, arg...)
}

func transcodeURL(dst io.Writer, inputURL *url.URL, arg ...string) (time.Duration, error) {
	arg = append([]string{"-i", inputURL.String()}, arg...)
	return runFfmpegCommand(dst, nil, arg...)
}

func transcodeToBucket(bucketName, filename, format string, input interface{}, arg ...string) (result *transcodeResult, err error) {
	// Create a random filename.
	tmpNameBytes := make([]byte, 32)
	var n int
	n, err = rand.Read(tmpNameBytes)
	if err != nil {
		return
	} else if n != 32 {
		err = fmt.Errorf("failed to create random name")
		return
	}
	tmpName := fmt.Sprintf("%s_t.%s", hex.EncodeToString(tmpNameBytes), format)
	tmpPath := filepath.Join(os.TempDir(), tmpName)
	// Use the random filename as the final name if one was not provided.
	if filename == "" {
		filename = tmpName
	}
	// Create a writer which will write into the specified bucket and filename.
	w, err := writerForBucket(bucketName, filename)
	if err != nil {
		return
	}
	// Prepend the -f <format> argument before passing on the arguments.
	arg = append([]string{"-f", format}, arg...)
	// We'd prefer to pipe directly to the Storage writer, but MP4 files cannot
	// be piped due to seeking requirements. For MP4, we need to use a file.
	var tw io.Writer
	if format == "mp4" {
		arg = append(arg, tmpPath)
	} else {
		tw = w
		arg = append(arg, "pipe:1")
	}
	// Transcode from either a stream or a URL.
	var duration time.Duration
	switch input := input.(type) {
	case io.Reader:
		duration, err = transcodeStream(tw, input, arg...)
	case *url.URL:
		duration, err = transcodeURL(tw, input, arg...)
	}
	if err != nil {
		w.CloseWithError(err)
		return
	}
	// Upload from disk for MP4 files.
	if format == "mp4" {
		defer os.Remove(tmpName)
		r, err := os.Open(tmpPath)
		if err != nil {
			w.CloseWithError(err)
			return nil, err
		}
		defer r.Close()
		_, err = io.Copy(w, r)
		if err != nil {
			w.CloseWithError(err)
			return nil, err
		}
	}
	err = w.Close()
	if err != nil {
		return
	}
	result = &transcodeResult{
		BucketPath: fmt.Sprintf("%s/%s", bucketName, filename),
		Duration:   int64(duration / time.Millisecond),
	}
	return
}

func writerForBucket(bucketName, filename string) (w *storage.Writer, err error) {
	bucket, ok := buckets[bucketName]
	if !ok {
		err = fmt.Errorf("invalid bucket %s", bucketName)
		return
	}
	w = bucket.Object(filename).NewWriter(ctx)
	w.ACL = []storage.ACLRule{{Entity: storage.AllUsers, Role: storage.RoleReader}}
	return
}
