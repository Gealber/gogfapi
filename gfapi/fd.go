package gfapi

// This file includes lower level operations on fd like the ones in the 'syscall' package

// #cgo pkg-config: glusterfs-api
// #include "glusterfs/api/glfs.h"
// #include <stdlib.h>
// #include <sys/stat.h>
import "C"
import (
	"os"
	"syscall"
	"time"
	"unsafe"
)

// Fd is the glusterfs fd type
type Fd struct {
	fd *C.glfs_fd_t
}

type Stat struct {
	// What results were written [uncond]
	mask uint64
	// Flags conveying information about the file [uncond]
	attributes uint64
	// Mask to show what's supported in st_attributes [ucond]
	attributesMask uint64
	// Last access time
	atime time.Time
	// File creation time
	btime time.Time
	// Last attribute change time
	ctime time.Time
	// Last data modification time
	mtime time.Time
	// Inode number
	ino uint64
	// File size
	size int64
	// Number of 512-byte blocks allocated
	blocks uint64
	// Device ID of special file [if bdev/cdev]
	rdevMajor uint32
	rdevMinor uint32
	// ID of device containing file [uncond]
	devMajor uint32
	devMinor uint32
	// Preferred general I/O size [uncond]
	blkksize int64
	// Number of hard links
	nlink uint64
	// User ID of owner
	uid uint32
	// Group ID of owner
	gid uint32
	// File mode
	mode uint32
}

func (s *Stat) ToGlfsStat() *C.struct_glfs_stat {
	if s == nil {
		return nil
	}

	return &C.struct_glfs_stat{
		glfs_st_mask:            C.ulong(s.mask),
		glfs_st_attributes:      C.ulong(s.attributes),
		glfs_st_attributes_mask: C.ulong(s.attributesMask),
		glfs_st_atime: C.struct_timespec{
			tv_sec:  C.__time_t(s.atime.Unix()),
			tv_nsec: C.__syscall_slong_t(s.atime.Nanosecond()),
		},
		glfs_st_btime: C.struct_timespec{
			tv_sec:  C.__time_t(s.btime.Unix()),
			tv_nsec: C.__syscall_slong_t(s.btime.Nanosecond()),
		},
		glfs_st_ctime: C.struct_timespec{
			tv_sec:  C.__time_t(s.ctime.Unix()),
			tv_nsec: C.__syscall_slong_t(s.ctime.Nanosecond()),
		},
		glfs_st_ino:        C.ulong(s.ino),
		glfs_st_size:       C.long(s.size),
		glfs_st_blocks:     C.long(s.blocks),
		glfs_st_rdev_major: C.uint(s.rdevMajor),
		glfs_st_rdev_minor: C.uint(s.rdevMinor),
		glfs_st_dev_major:  C.uint(s.devMajor),
		glfs_st_dev_minor:  C.uint(s.devMinor),
		glfs_st_nlink:      C.ulong(s.nlink),
		glfs_st_uid:        C.uint(s.uid),
		glfs_st_gid:        C.uint(s.gid),
		glfs_st_mode:       C.uint(s.mode),
	}
}

var _zero uintptr

// Fchmod changes the mode of the Fd to the given mode
//
// Returns error on failure
func (fd *Fd) Fchmod(mode uint32) error {
	_, err := C.glfs_fchmod(fd.fd, C.mode_t(mode))

	return err
}

// Fstat performs an fstat call on the Fd and saves stat details in the passed stat structure
//
// Returns error on failure
func (fd *Fd) Fstat(stat *syscall.Stat_t) error {

	ret, err := C.glfs_fstat(fd.fd, (*C.struct_stat)(unsafe.Pointer(stat)))
	if int(ret) < 0 {
		return err
	}
	return nil
}

// Fsync performs an fsync on the Fd
//
// Returns error on failure
func (fd *Fd) Fsync(prestat, poststat *C.struct_glfs_stat) error {
	ret, err := C.glfs_fsync(fd.fd, prestat, poststat)
	if ret < 0 {
		return err
	}
	return nil
}

// Ftruncate truncates the size of the Fd to the given size
//
// Returns error on failure
func (fd *Fd) Ftruncate(size int64, prestat, poststat *C.struct_glfs_stat) error {
	_, err := C.glfs_ftruncate(fd.fd, C.off_t(size), prestat, poststat)

	return err
}

// Pread reads at most len(b) bytes into b from offset off in Fd
//
// Returns number of bytes read on success and error on failure
func (fd *Fd) Pread(b []byte, off int64, poststat *C.struct_glfs_stat) (int, error) {
	n, err := C.glfs_pread(fd.fd, unsafe.Pointer(&b[0]), C.size_t(len(b)), C.off_t(off), 0, poststat)

	return int(n), err
}

// Pwrite writes len(b) bytes from b into the Fd from offset off
//
// Returns number of bytes written on success and error on failure
func (fd *Fd) Pwrite(b []byte, off int64, prestat, poststat *C.struct_glfs_stat) (int, error) {
	n, err := C.glfs_pwrite(fd.fd, unsafe.Pointer(&b[0]), C.size_t(len(b)), C.off_t(off), 0, prestat, poststat)

	return int(n), err
}

// Read reads at most len(b) bytes into b from Fd
//
// Returns number of bytes read on success and error on failure
func (fd *Fd) Read(b []byte) (n int, err error) {
	var p0 unsafe.Pointer

	if len(b) > 0 {
		p0 = unsafe.Pointer(&b[0])
	} else {
		p0 = unsafe.Pointer(&_zero)
	}

	// glfs_read returns a ssize_t. The value of which is the number of bytes written.
	// Unless, ret is -1, an error, implying to check errno. cgo collects errno as the
	// functions error return value.
	ret, e1 := C.glfs_read(fd.fd, p0, C.size_t(len(b)), 0)
	n = int(ret)
	if n < 0 {
		err = e1
	}

	return n, err
}

// Write writes len(b) bytes from b into the Fd
//
// Returns number of bytes written on success and error on failure
func (fd *Fd) Write(b []byte) (n int, err error) {
	var p0 unsafe.Pointer

	if len(b) > 0 {
		p0 = unsafe.Pointer(&b[0])
	} else {
		p0 = unsafe.Pointer(&_zero)
	}

	// glfs_write returns a ssize_t. The value of which is the number of bytes written.
	// Unless, ret is -1, an error, implying to check errno. cgo collects errno as the
	// functions error return value.
	ret, e1 := C.glfs_write(fd.fd, p0, C.size_t(len(b)), 0)
	n = int(ret)
	if n < 0 {
		err = e1
	}

	return n, err
}

func (fd *Fd) lseek(offset int64, whence int) (int64, error) {
	ret, err := C.glfs_lseek(fd.fd, C.off_t(offset), C.int(whence))

	return int64(ret), err
}

func (fd *Fd) Fallocate(mode int, offset int64, len int64) error {
	ret, err := C.glfs_fallocate(fd.fd, C.int(mode),
		C.off_t(offset), C.size_t(len))

	if ret == 0 {
		err = nil
	}
	return err
}

func (fd *Fd) Fgetxattr(attr string, dest []byte) (int64, error) {
	var ret C.ssize_t
	var err error

	cattr := C.CString(attr)
	defer C.free(unsafe.Pointer(cattr))

	if len(dest) <= 0 {
		ret, err = C.glfs_fgetxattr(fd.fd, cattr, nil, 0)
	} else {
		ret, err = C.glfs_fgetxattr(fd.fd, cattr,
			unsafe.Pointer(&dest[0]), C.size_t(len(dest)))
	}

	if ret >= 0 {
		return int64(ret), nil
	} else {
		return int64(ret), err
	}
}

func (fd *Fd) Fsetxattr(attr string, data []byte, flags int) error {

	cattr := C.CString(attr)
	defer C.free(unsafe.Pointer(cattr))

	ret, err := C.glfs_fsetxattr(fd.fd, cattr,
		unsafe.Pointer(&data[0]), C.size_t(len(data)),
		C.int(flags))

	if ret == 0 {
		err = nil
	}
	return err
}

func (fd *Fd) Fremovexattr(attr string) error {

	cattr := C.CString(attr)
	defer C.free(unsafe.Pointer(cattr))

	ret, err := C.glfs_fremovexattr(fd.fd, cattr)

	if ret == 0 {
		err = nil
	}
	return err
}

func direntName(dirent *syscall.Dirent) string {
	name := make([]byte, 0, len(dirent.Name))
	for i, c := range dirent.Name {
		if c == 0 || i > 255 {
			break
		}

		name = append(name, byte(c))
	}

	return string(name)
}

// Readdir returns the information of files in a directory.
//
// n is the maximum number of items to return. If there are more items than
// the maximum they can be obtained in successive calls. If maximum is 0
// then all the items will be returned.
func (fd *Fd) Readdir(n int) ([]os.FileInfo, error) {
	var (
		stat  syscall.Stat_t
		files []os.FileInfo
		statP = (*C.struct_stat)(unsafe.Pointer(&stat))
	)

	for i := 0; n == 0 || i < n; i++ {
		d, err := C.glfs_readdirplus(fd.fd, statP)
		if err != nil {
			return nil, err
		}

		dirent := (*syscall.Dirent)(unsafe.Pointer(d))
		if dirent == nil {
			break
		}

		name := direntName(dirent)
		file := fileInfoFromStat(&stat, name)
		files = append(files, file)
	}

	return files, nil
}

// Readdirnames returns the names of files in a directory.
//
// n is the maximum number of items to return and works the same way as Readdir.
func (fd *Fd) Readdirnames(n int) ([]string, error) {
	var names []string

	for i := 0; n == 0 || i < n; i++ {
		d, err := C.glfs_readdir(fd.fd)
		if err != nil {
			return nil, err
		}

		dirent := (*syscall.Dirent)(unsafe.Pointer(d))
		if dirent == nil {
			break
		}

		name := direntName(dirent)
		names = append(names, name)
	}

	return names, nil
}
