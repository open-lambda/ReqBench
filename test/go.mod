module test

go 1.18

replace rb => ../src/ //TODO: no longer needed, if we switch to github url

require (
	golang.org/x/mod v0.14.0 // indirect
	golang.org/x/tools v0.17.0 // indirect
	rb v0.0.0-00010101000000-000000000000 // indirect
)
