package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func AvailableDiskSize() (uint64, error) {
	var stat syscall.Statfs_t
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}

	if err := syscall.Statfs(wd, &stat); err != nil {
		return 0, err
	}

	return stat.Bavail * uint64(stat.Bsize), nil
}

func CopyDir(src, dest string, excluded []string) error {

	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1)

		if fileName == "" {
			return nil
		}

		for _, e := range excluded {
			matched, err := filepath.Match(e, info.Name())

			if err != nil {
				return err
			}

			if matched {
				return nil
			}

		}

		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}

		data, err := os.ReadFile(filepath.Join(src, fileName))

		if err != nil {
			return err
		}

		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())

	})

}
