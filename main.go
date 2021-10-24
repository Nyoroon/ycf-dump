package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/lockbox/v1"
	ycsdk "github.com/yandex-cloud/go-sdk"
	"golang.org/x/sync/errgroup"
)

func DumpDir(w io.Writer, dumpPath string) error {
	dumpPathFS := os.DirFS(dumpPath)
	archW := tar.NewWriter(w)

	err := fs.WalkDir(dumpPathFS, ".", func(p string, d fs.DirEntry, err error) error {
		if strings.HasPrefix(p, "dev/") ||
			strings.HasPrefix(p, "proc/") ||
			strings.HasPrefix(p, "sys/") ||
			p == "." {
			return nil
		}

		if err != nil && !errors.Is(err, fs.ErrPermission) {
			return err
		}

		fileInfo, err := d.Info()
		if errors.Is(err, fs.ErrNotExist) {
			log.Printf("%s: not exists", path.Join(dumpPath, p))
			return nil
		}
		if err != nil {
			return fmt.Errorf("error getting file info: %w", err)
		}

		var link string
		switch {
		case fileInfo.Mode()&fs.ModeSymlink != 0:
			link, err = os.Readlink(path.Join(dumpPath, p))
			if errors.Is(err, os.ErrPermission) {
				link = "permission denied"
			} else if err != nil {
				return fmt.Errorf("error reading link: %w", err)
			}
		case fileInfo.Mode()&fs.ModeSocket != 0:
			log.Printf("%s: skipping socket", path.Join(dumpPath, p))
			return nil
		}

		hdr, err := tar.FileInfoHeader(fileInfo, link)
		if err != nil {
			return fmt.Errorf("error generating tar header: %w", err)
		}

		hdr.Name = p

		fileReadable := (fileInfo.Mode()&fs.ModePerm)&0004 != 0
		if !fileReadable {
			hdr.Size = 0
		}

		if err := archW.WriteHeader(hdr); err != nil {
			return fmt.Errorf("error writing header: %w", err)
		}

		if !fileInfo.Mode().IsRegular() || !fileReadable {
			return nil
		}

		f, err := os.Open(path.Join(dumpPath, p))
		if err != nil {
			return fmt.Errorf("error reading file: %w", err)
		}
		defer f.Close()

		if _, err := io.Copy(archW, f); err != nil {
			return fmt.Errorf("can't copy file: %w", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("error dumping dir: %w", err)
	}

	return archW.Close()
}

func getSecret(ctx context.Context, sdk *ycsdk.SDK) (map[string]string, error) {
	secretID := os.Getenv("SECRET_ID")
	payload, err := sdk.LockboxPayload().Payload().Get(
		ctx,
		&lockbox.GetPayloadRequest{
			SecretId: secretID,
		},
	)
	if err != nil {
		return nil, err
	}

	log.Printf("got secrets from %s version %s", secretID, payload.VersionId)

	secrets := make(map[string]string, len(payload.Entries))
	for _, entry := range payload.Entries {
		secrets[entry.GetKey()] = entry.GetTextValue()
	}

	return secrets, nil
}

func NewS3Client(id, secret string) (*minio.Client, error) {
	const s3Endpoint = "storage.yandexcloud.net"

	opts := &minio.Options{
		Creds:  credentials.NewStaticV4(id, secret, ""),
		Secure: true,
		Region: os.Getenv("REGION"),
	}
	return minio.New(s3Endpoint, opts)
}

var _ http.HandlerFunc = Handler

func Handler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	sdk, err := ycsdk.Build(
		r.Context(),
		ycsdk.Config{
			Credentials: ycsdk.InstanceServiceAccount(),
		},
	)
	if err != nil {
		log.Println(err.Error())
		http.Error(w, "can't initalize sdk", http.StatusInternalServerError)
		return
	}

	secrets, err := getSecret(r.Context(), sdk)
	if err != nil {
		log.Println(err.Error())
		http.Error(w, "can't get secrets", http.StatusInternalServerError)
		return
	}

	s3client, err := NewS3Client(secrets["AWS_ACCESS_KEY"], secrets["AWS_SECRET_KEY"])
	if err != nil {
		log.Println(err.Error())
		http.Error(w, "s3 unavailable", http.StatusInternalServerError)
		return
	}

	errg, ctx := errgroup.WithContext(r.Context())
	pipeR, pipeW := io.Pipe()

	var putInfo minio.UploadInfo
	errg.Go(func() error {
		var err error
		putInfo, err = s3client.PutObject(
			ctx,
			os.Getenv("BUCKET"),
			fmt.Sprintf("go/%s/dump.tar.gz", time.Now().Format(time.RFC3339)),
			pipeR,
			-1,
			minio.PutObjectOptions{
				ContentType:     "application/x-tar",
				ContentEncoding: "gzip",
				PartSize:        5 * 1024 * 1024,
			},
		)

		return err
	})

	errg.Go(func() error {
		gzipW, _ := gzip.NewWriterLevel(pipeW, gzip.BestSpeed)

		err := DumpDir(gzipW, "/")
		gzipW.Close()
		if err != nil {
			_ = pipeW.CloseWithError(err)
		} else {
			pipeW.Close()
		}
		return err
	})

	err = errg.Wait()

	w.Header().Set("Server-Timing", fmt.Sprintf("total;dur=%.3f", time.Since(start).Seconds()))

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	presigned, err := s3client.PresignedGetObject(
		ctx,
		putInfo.Bucket,
		putInfo.Key,
		15*time.Minute,
		url.Values{},
	)
	if err != nil {
		fmt.Fprintf(w, "error generating presigned url: %s", err.Error())
		return
	}
	fmt.Fprintln(w, presigned.String())
}
