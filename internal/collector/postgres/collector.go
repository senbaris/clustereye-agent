package postgres

import (
	"fmt"
	"os/exec"
)

func GetPGServiceStatus() string {

	out, err := exec.Command("sh", "-c", "ps -aux | grep postgres: | grep -v grep").Output()
	if err != nil || len(out) == 0 {
		fmt.Println(out, err)
		return "FAIL!"
	}
	return "RUNNING"

}
