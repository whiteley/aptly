package gcs

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/aptly-dev/aptly/aptly"
	"github.com/aptly-dev/aptly/utils"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

// PublishedStorage abstract file system with published files (actually hosted on GCS)
type PublishedStorage struct {
	gcs        *storage.Client
	bucketName string
	prefix     string
	pathCache  map[string]string
}

// Check interface
var (
	_ aptly.PublishedStorage = (*PublishedStorage)(nil)
)

// NewPublishedStorage creates published storage from raw gcp credentials
func NewPublishedStorage(bucketName string, prefix string) (*PublishedStorage, error) {
	gcs, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, fmt.Errorf("Unable to create storage service: %v", err)
	}

	return &PublishedStorage{gcs, bucketName, prefix, nil}, nil
}

func (publishedStorage *PublishedStorage) String() string {
	return fmt.Sprintf("GCS: %s:%s", publishedStorage.bucketName, publishedStorage.prefix)
}

// MkDir creates directory recursively under public path
func (publishedStorage *PublishedStorage) MkDir(path string) error {
	// no op for GCS
	return nil
}

// PutFile puts file into published storage at specified path
func (publishedStorage *PublishedStorage) PutFile(path string, sourceFilename string) error {
	source, err := os.Open(sourceFilename)
	defer source.Close()
	if err != nil {
		return err
	}

	err = publishedStorage.putFile(path, source)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("error uploading %s to %s", sourceFilename, publishedStorage))
	}

	return err
}

func (publishedStorage *PublishedStorage) putFile(path string, source io.ReadSeeker) error {
	ctx := context.Background()
	key := filepath.Join(publishedStorage.prefix, path)

	wc := publishedStorage.gcs.Bucket(publishedStorage.bucketName).Object(key).NewWriter(ctx)
	if _, err := io.Copy(wc, source); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}

	return nil
}

// Remove removes single file under public path
func (publishedStorage *PublishedStorage) Remove(path string) error {
	ctx := context.Background()
	key := filepath.Join(publishedStorage.prefix, path)

	o := publishedStorage.gcs.Bucket(publishedStorage.bucketName).Object(key)
	if err := o.Delete(ctx); err != nil {
		if err == storage.ErrBucketNotExist {
			// ignore 'no such bucket' errors on removal
			return nil
		}
		return fmt.Errorf("error deleting %s from %s: %s", key, publishedStorage, err)
	}

	return nil
}

// RemoveDirs removes directory structure under public path
func (publishedStorage *PublishedStorage) RemoveDirs(path string, progress aptly.Progress) error {
	filelist, _, err := publishedStorage.internalFilelist(path)
	if err != nil {
		if err == storage.ErrBucketNotExist {
			// ignore 'no such bucket' errors on removal
			return nil
		}
		return err
	}

	for i := range filelist {
		err := publishedStorage.Remove(filepath.Join(publishedStorage.prefix, path, filelist[i]))
		if err != nil {
			return fmt.Errorf("error deleting path %s from %s: %s", filelist[i], publishedStorage, err)
		}
	}
	return nil
}

// LinkFromPool links package file from pool to dist's pool location
//
// publishedDirectory is desired location in pool (like prefix/pool/component/liba/libav/)
// sourcePool is instance of aptly.PackagePool
// sourcePath is filepath to package file in package pool
//
// LinkFromPool returns relative path for the published file to be included in package index
func (publishedStorage *PublishedStorage) LinkFromPool(publishedDirectory, fileName string, sourcePool aptly.PackagePool,
	sourcePath string, sourceChecksums utils.ChecksumInfo, force bool) error {
	relPath := filepath.Join(publishedDirectory, fileName)
	poolPath := filepath.Join(publishedStorage.prefix, relPath)

	if publishedStorage.pathCache == nil {
		paths, md5s, err := publishedStorage.internalFilelist("")
		if err != nil {
			return errors.Wrap(err, "error caching paths under prefix")
		}

		publishedStorage.pathCache = make(map[string]string, len(paths))

		for i := range paths {
			publishedStorage.pathCache[paths[i]] = md5s[i]
		}
	}

	destinationMD5, exists := publishedStorage.pathCache[relPath]
	sourceMD5 := sourceChecksums.MD5

	if exists {
		if sourceMD5 == "" {
			return fmt.Errorf("unable to compare object, MD5 checksum missing")
		}

		if destinationMD5 == sourceMD5 {
			return nil
		}

		if !force && destinationMD5 != sourceMD5 {
			return fmt.Errorf("error putting file to %s: file already exists and is different: %s %s %s", poolPath, publishedStorage, destinationMD5, sourceMD5)

		}
	}

	source, err := sourcePool.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()

	err = publishedStorage.putFile(relPath, source)
	if err == nil {
		publishedStorage.pathCache[relPath] = sourceMD5
	} else {
		err = errors.Wrap(err, fmt.Sprintf("error uploading %s to %s: %s", sourcePath, publishedStorage, poolPath))
	}

	return err
}

func (publishedStorage *PublishedStorage) attrs(path string) (*storage.ObjectAttrs, error) {
	ctx := context.Background()

	o := publishedStorage.gcs.Bucket(publishedStorage.bucketName).Object(path)
	attrs, err := o.Attrs(ctx)
	if err != nil {
		return nil, err
	}

	return attrs, nil
}

// Filelist returns list of files under prefix
func (publishedStorage *PublishedStorage) Filelist(prefix string) ([]string, error) {
	paths, _, err := publishedStorage.internalFilelist(prefix)
	return paths, err
}

func (publishedStorage *PublishedStorage) internalFilelist(prefix string) (paths []string, md5s []string, err error) {
	ctx := context.Background()

	paths = make([]string, 0, 1024)
	md5s = make([]string, 0, 1024)
	prefix = filepath.Join(publishedStorage.prefix, prefix)
	if prefix != "" {
		prefix += "/"
	}

	it := publishedStorage.gcs.Bucket(publishedStorage.bucketName).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, errors.WithMessagef(err, "error listing under prefix %s in %s: %s", prefix, publishedStorage, err)
		}
		if prefix == "" {
			paths = append(paths, attrs.Name)
		} else {
			paths = append(paths, (attrs.Name)[len(prefix):])
		}
		md5s = append(md5s, hex.EncodeToString(attrs.MD5))
	}
	return paths, md5s, nil
}

// RenameFile renames (moves) file
func (publishedStorage *PublishedStorage) RenameFile(oldName, newName string) error {
	ctx := context.Background()

	sourcePath := filepath.Join(publishedStorage.prefix, oldName)
	destPath := filepath.Join(publishedStorage.prefix, newName)

	src := publishedStorage.gcs.Bucket(publishedStorage.bucketName).Object(sourcePath)
	dst := publishedStorage.gcs.Bucket(publishedStorage.bucketName).Object(destPath)

	if _, err := dst.CopierFrom(src).Run(ctx); err != nil {
		return err
	}
	if err := src.Delete(ctx); err != nil {
		return err
	}

	return nil
}

// SymLink creates a copy of src file and adds link information as meta data
func (publishedStorage *PublishedStorage) SymLink(src string, dst string) error {
	ctx := context.Background()

	sourcePath := filepath.Join(publishedStorage.prefix, src)
	destPath := filepath.Join(publishedStorage.prefix, dst)

	err := publishedStorage.copy(sourcePath, destPath)
	if err != nil {
		return fmt.Errorf("error symlinking %s -> %s in %s: %s", src, dst, publishedStorage, err)
	}
	_, err = publishedStorage.gcs.Bucket(publishedStorage.bucketName).Object(destPath).Update(ctx, storage.ObjectAttrsToUpdate{
		Metadata: map[string]string{
			"SymLink": sourcePath,
		},
	})
	if err != nil {
		return fmt.Errorf("error symlinking %s -> %s in %s: %s", src, dst, publishedStorage, err)
	}

	return nil
}

func (publishedStorage *PublishedStorage) copy(src string, dst string) error {
	ctx := context.Background()

	srcObj := publishedStorage.gcs.Bucket(publishedStorage.bucketName).Object(src)
	dstObj := publishedStorage.gcs.Bucket(publishedStorage.bucketName).Object(dst)

	if _, err := dstObj.CopierFrom(srcObj).Run(ctx); err != nil {
		return err
	}

	return nil
}

// HardLink using symlink functionality as hard links do not exist
func (publishedStorage *PublishedStorage) HardLink(src string, dst string) error {
	return publishedStorage.SymLink(src, dst)
}

// FileExists returns true if path exists
func (publishedStorage *PublishedStorage) FileExists(path string) (bool, error) {
	key := filepath.Join(publishedStorage.prefix, path)

	_, err := publishedStorage.attrs(key)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ReadLink returns the symbolic link pointed to by path.
// This simply reads text file created with SymLink
func (publishedStorage *PublishedStorage) ReadLink(path string) (string, error) {
	key := filepath.Join(publishedStorage.prefix, path)

	attrs, err := publishedStorage.attrs(key)
	if err != nil {
		return "", err
	}

	if link, ok := attrs.Metadata["SymLink"]; ok {
		return link, nil
	}
	return "", fmt.Errorf("error getting information about %s from %s: %s", path, publishedStorage, err)
}
