package gcs

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/aptly-dev/aptly/files"
	"github.com/aptly-dev/aptly/utils"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	. "gopkg.in/check.v1"
)

type PublishedStorageSuite struct {
	srv                               *fakestorage.Server
	publishedStorage, prefixedStorage *PublishedStorage
	noSuchBucketStorage               *PublishedStorage
}

var _ = Suite(&PublishedStorageSuite{})

func (s *PublishedStorageSuite) SetUpTest(c *C) {
	s.srv = fakestorage.NewServer([]fakestorage.Object{})
	s.srv.CreateBucket("test")
	s.publishedStorage = &PublishedStorage{s.srv.Client(), "test", "", nil}
	s.prefixedStorage = &PublishedStorage{s.srv.Client(), "test", "lala", nil}
	s.noSuchBucketStorage = &PublishedStorage{s.srv.Client(), "no-bucket", "", nil}
}

func (s *PublishedStorageSuite) TearDownTest(c *C) {
	s.srv.Stop()
}

func (s *PublishedStorageSuite) GetFile(c *C, path string) []byte {
	ctx := context.Background()

	rc, err := s.publishedStorage.gcs.Bucket(s.publishedStorage.bucketName).Object(path).NewReader(ctx)
	c.Assert(err, IsNil)
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	c.Assert(err, IsNil)

	return data
}

func (s *PublishedStorageSuite) AssertNoFile(c *C, path string) {
	_, err := s.publishedStorage.attrs(path)
	c.Assert(err, ErrorMatches, "storage: object doesn't exist")
}

func (s *PublishedStorageSuite) PutFile(c *C, path string, data []byte) {
	h := md5.New()
	h.Write(data)

	s.srv.CreateObject(fakestorage.Object{
		BucketName: s.publishedStorage.bucketName,
		Name:       path,
		Content:    data,
		Md5Hash:    base64.StdEncoding.EncodeToString(h.Sum(nil)),
	})
}

func (s *PublishedStorageSuite) TestPutFile(c *C) {
	dir := c.MkDir()
	err := ioutil.WriteFile(filepath.Join(dir, "a"), []byte("welcome to gcs!"), 0644)
	c.Assert(err, IsNil)

	err = s.publishedStorage.PutFile("a/b.txt", filepath.Join(dir, "a"))
	c.Check(err, IsNil)

	c.Check(s.GetFile(c, "a/b.txt"), DeepEquals, []byte("welcome to gcs!"))

	err = s.prefixedStorage.PutFile("a/b.txt", filepath.Join(dir, "a"))
	c.Check(err, IsNil)

	c.Check(s.GetFile(c, "lala/a/b.txt"), DeepEquals, []byte("welcome to gcs!"))
}

func (s *PublishedStorageSuite) TestFilelist(c *C) {
	paths := []string{"a", "b", "c", "testa", "test/a", "test/b", "lala/a", "lala/b", "lala/c"}
	for _, path := range paths {
		s.PutFile(c, path, []byte("test"))
	}

	list, err := s.publishedStorage.Filelist("")
	c.Check(err, IsNil)
	c.Check(list, DeepEquals, []string{"a", "b", "c", "lala/a", "lala/b", "lala/c", "test/a", "test/b", "testa"})

	list, err = s.publishedStorage.Filelist("test")
	c.Check(err, IsNil)
	c.Check(list, DeepEquals, []string{"a", "b"})

	list, err = s.publishedStorage.Filelist("test2")
	c.Check(err, IsNil)
	c.Check(list, DeepEquals, []string{})

	list, err = s.prefixedStorage.Filelist("")
	c.Check(err, IsNil)
	c.Check(list, DeepEquals, []string{"a", "b", "c"})
}

func (s *PublishedStorageSuite) TestRemove(c *C) {
	s.PutFile(c, "a/b", []byte("test"))

	err := s.publishedStorage.Remove("a/b")
	c.Check(err, IsNil)

	s.AssertNoFile(c, "a/b")

	s.PutFile(c, "lala/xyz", []byte("test"))

	errp := s.prefixedStorage.Remove("xyz")
	c.Check(errp, IsNil)

	s.AssertNoFile(c, "lala/xyz")
}

func (s *PublishedStorageSuite) TestRemoveNoSuchBucket(c *C) {
	err := s.noSuchBucketStorage.Remove("a/b")
	c.Check(err, IsNil)
	c.Skip("gcs returns no such object error")
}

func (s *PublishedStorageSuite) TestRemoveDirs(c *C) {
	paths := []string{"a", "b", "c", "testa", "test/a+1", "test/a 1", "lala/a+b", "lala/a b", "lala/c"}
	for _, path := range paths {
		s.PutFile(c, path, []byte("test"))
	}

	err := s.publishedStorage.RemoveDirs("test", nil)
	c.Check(err, IsNil)

	list, err := s.publishedStorage.Filelist("")
	c.Check(err, IsNil)
	c.Check(list, DeepEquals, []string{"a", "b", "c", "lala/a b", "lala/a+b", "lala/c", "testa"})
}

func (s *PublishedStorageSuite) TestRemoveDirsNoSuchBucket(c *C) {
	err := s.noSuchBucketStorage.RemoveDirs("a/b", nil)
	c.Check(err, IsNil)
	c.Skip("gcs returns no such object error")
}

func (s *PublishedStorageSuite) TestRenameFile(c *C) {
	s.PutFile(c, "a/b", []byte("test"))
	err := s.publishedStorage.RenameFile("a/b", "b/c")
	c.Check(err, IsNil)
	s.AssertNoFile(c, "a/b")
	c.Check(s.GetFile(c, "b/c"), DeepEquals, []byte("test"))
}

func (s *PublishedStorageSuite) TestLinkFromPool(c *C) {
	root := c.MkDir()
	pool := files.NewPackagePool(root, false)
	cs := files.NewMockChecksumStorage()

	tmpFile1 := filepath.Join(c.MkDir(), "mars-invaders_1.03.deb")
	err := ioutil.WriteFile(tmpFile1, []byte("Contents"), 0644)
	c.Assert(err, IsNil)
	cksum1 := utils.ChecksumInfo{MD5: "c1df1da7a1ce305a3b60af9d5733ac1d"}

	tmpFile2 := filepath.Join(c.MkDir(), "mars-invaders_1.03.deb")
	err = ioutil.WriteFile(tmpFile2, []byte("Spam"), 0644)
	c.Assert(err, IsNil)
	cksum2 := utils.ChecksumInfo{MD5: "e9dfd31cc505d51fc26975250750deab"}

	tmpFile3 := filepath.Join(c.MkDir(), "netboot/boot.img.gz")
	os.MkdirAll(filepath.Dir(tmpFile3), 0777)
	err = ioutil.WriteFile(tmpFile3, []byte("Contents"), 0644)
	c.Assert(err, IsNil)
	cksum3 := utils.ChecksumInfo{MD5: "c1df1da7a1ce305a3b60af9d5733ac1d"}

	src1, err := pool.Import(tmpFile1, "mars-invaders_1.03.deb", &cksum1, true, cs)
	c.Assert(err, IsNil)
	src2, err := pool.Import(tmpFile2, "mars-invaders_1.03.deb", &cksum2, true, cs)
	c.Assert(err, IsNil)
	src3, err := pool.Import(tmpFile3, "netboot/boot.img.gz", &cksum3, true, cs)
	c.Assert(err, IsNil)

	// first link from pool
	err = s.publishedStorage.LinkFromPool(filepath.Join("", "pool", "main", "m/mars-invaders"), "mars-invaders_1.03.deb", pool, src1, cksum1, false)
	c.Check(err, IsNil)

	c.Check(s.GetFile(c, "pool/main/m/mars-invaders/mars-invaders_1.03.deb"), DeepEquals, []byte("Contents"))

	// duplicate link from pool
	err = s.publishedStorage.LinkFromPool(filepath.Join("", "pool", "main", "m/mars-invaders"), "mars-invaders_1.03.deb", pool, src1, cksum1, false)
	c.Check(err, IsNil)

	c.Check(s.GetFile(c, "pool/main/m/mars-invaders/mars-invaders_1.03.deb"), DeepEquals, []byte("Contents"))

	// link from pool with conflict
	err = s.publishedStorage.LinkFromPool(filepath.Join("", "pool", "main", "m/mars-invaders"), "mars-invaders_1.03.deb", pool, src2, cksum2, false)
	c.Check(err, ErrorMatches, ".*file already exists and is different.*")

	c.Check(s.GetFile(c, "pool/main/m/mars-invaders/mars-invaders_1.03.deb"), DeepEquals, []byte("Contents"))

	// link from pool with conflict and force
	err = s.publishedStorage.LinkFromPool(filepath.Join("", "pool", "main", "m/mars-invaders"), "mars-invaders_1.03.deb", pool, src2, cksum2, true)
	c.Check(err, IsNil)

	c.Check(s.GetFile(c, "pool/main/m/mars-invaders/mars-invaders_1.03.deb"), DeepEquals, []byte("Spam"))

	// for prefixed storage:
	// first link from pool
	err = s.prefixedStorage.LinkFromPool(filepath.Join("", "pool", "main", "m/mars-invaders"), "mars-invaders_1.03.deb", pool, src1, cksum1, false)
	c.Check(err, IsNil)

	// 2nd link from pool, providing wrong path for source file
	//
	// this test should check that file already exists in GCS and skip upload (which would fail if not skipped)
	s.prefixedStorage.pathCache = nil
	err = s.prefixedStorage.LinkFromPool(filepath.Join("", "pool", "main", "m/mars-invaders"), "mars-invaders_1.03.deb", pool, "wrong-looks-like-pathcache-doesnt-work", cksum1, false)
	c.Check(err, IsNil)

	c.Check(s.GetFile(c, "lala/pool/main/m/mars-invaders/mars-invaders_1.03.deb"), DeepEquals, []byte("Contents"))

	// link from pool with nested file name
	err = s.publishedStorage.LinkFromPool("dists/jessie/non-free/installer-i386/current/images", "netboot/boot.img.gz", pool, src3, cksum3, false)
	c.Check(err, IsNil)

	c.Check(s.GetFile(c, "dists/jessie/non-free/installer-i386/current/images/netboot/boot.img.gz"), DeepEquals, []byte("Contents"))
}

func (s *PublishedStorageSuite) TestSymLink(c *C) {
	s.PutFile(c, "a/b", []byte("test"))

	err := s.publishedStorage.SymLink("a/b", "a/b.link")
	c.Check(err, IsNil)

	var link string
	link, err = s.publishedStorage.ReadLink("a/b.link")
	c.Check(err, IsNil)
	c.Check(link, Equals, "a/b")
	c.Skip("Metadata attribute not supported in fake-gcs-server")
}

func (s *PublishedStorageSuite) TestFileExists(c *C) {
	s.PutFile(c, "a/b", []byte("inside the file"))

	exists1, err := s.publishedStorage.FileExists("a/b")
	c.Check(err, IsNil)
	c.Check(exists1, Equals, true)

	exists2, err := s.publishedStorage.FileExists("b/c")
	c.Check(err, IsNil)
	c.Check(exists2, Equals, false)
}
