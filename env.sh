
#
#/bin/bash

_print_help() {
    echo "This is a script to setup/tear down the test enviroment"
    echo "env up --- set up the env"
    echo "evn down --- tear down the env"
}

up() {
    sudo ip link add veth0 type veth peer name veth1
    sudo ip link add veth2 type veth peer name veth3
    sudo ip netns add vnet0
    sudo ip netns add vnet1
    sudo ifconfig veth0 hw ether aa:00:00:00:00:00
    sudo ifconfig veth3 hw ether aa:00:00:00:00:01
    sudo ip link set veth0 netns vnet0
    sudo ip link set veth3 netns vnet1
    sudo ip -n vnet0 link set veth0 up
    sudo ip -n vnet1 link set veth3 up
    sudo ip link set veth1 up
    sudo ip link set veth2 up
    sudo ip -n vnet0 addr add 10.0.0.1/24 dev veth0
    sudo ip -n vnet1 addr add 10.0.0.2/24 dev veth3

    # off the rx check
    sudo ip netns exec vnet0 ethtool --offload veth0 rx off tx off
    sudo ip netns exec vnet1 ethtool --offload veth3 rx off tx off
}

down() {
    # down env
    sudo ip -n vnet0 link set veth0 down
    sudo ip -n vnet1 link set veth3 down
    sudo ip link set veth1 down
    sudo ip link set veth2 down

    # rm interface
    sudo ip link del veth1
    sudo ip link del veth2

    # rm netns
    sudo ip netns del vnet0
    sudo ip netns del vnet1
}



_main() {
    case $1 in
    help | --help | -h)
        _print_help
        exit
        ;;
    -*)
        echo "invalid option \`$1\`"
        exit
        ;;
    *)
        # if [[ "$1" == "_"* || $(type -t "$1") != function ]]; then
        #     # Prevent to call invalid function
        #     echo "invalid command \`$1\`"
        #     exit 1
        # fi

        $@
        exit
        ;;
    esac

    _print_help
    exit 1
}

_main $@ 
