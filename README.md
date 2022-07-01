# QMonster: An AMQP 1.0 message server

An AMQP 1.0 message broker service.

## Requirements

*   [Qpid Proton](https://gitbox.apache.org/repos/asf/qpid-proton.git). Latest
    build from git repo necessary, release 0.37.0 does not work, as
    `pn_tostring` was added after this major release, which is necessary for
    [electron](https://pkg.go.dev/github.com/apache/qpid-proton/go/pkg/electron)
    to work.
