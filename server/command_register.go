package server

type CommandFunc func(c *Client) error

var commands = make(map[string]CommandFunc)

func commandRegister(commandName string, f CommandFunc) {
	if _, ok := commands[commandName]; ok {
		return
	}
	commands[commandName] = f
}
func getCommandFunc(commandName string) (f CommandFunc, ok bool) {
	f, ok = commands[commandName]
	return
}
