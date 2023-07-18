package prompt

import (
	"fmt"
	"syscall"

	"github.com/manifoldco/promptui"
)

var yesNoMapping = map[string]bool{
	"y":   true,
	"Y":   true,
	"yes": true,
	"n":   false,
	"N":   false,
	"no":  false,
}

// Confirm promps the user for a yes/no answer.
func Confirm(question string) bool {
	prompt := promptui.Prompt{
		Label:   fmt.Sprintf("%s: [y/N]?", question),
		Default: "n",
	}
	for {
		result, err := prompt.Run()
		if err != nil {
			if err == promptui.ErrInterrupt {
				// Propagate the interrupt.
				syscall.Kill(syscall.Getpid(), syscall.SIGINT)
			}
			continue
		}
		value, ok := yesNoMapping[result]
		if !ok {
			continue
		}
		return value
	}
}
