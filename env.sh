
#
#/bin/bash

up() {
    num=$1
    toml_file="env.toml"
    if [ -f "$toml_file" ]; then
        rm -f "$toml_file"
    fi

    echo "" > $toml_file

    for i in $(seq 0 $(($num - 1))); do
        sudo ip link add veth${i}-a type veth peer name veth${i}-b
        sudo ip netns add vnet$i
        sudo ifconfig veth${i}-a hw ether $(printf 'aa:00:00:00:00:%02x' $i)
        sudo ip link set veth${i}-a netns vnet$i
        sudo ip -n vnet$i link set veth${i}-a up
        sudo ip link set veth${i}-b up
        sudo ip -n vnet$i addr add 10.0.0.$(($i + 1))/24 dev veth${i}-a

        # close rx check for xdp
        sudo ip netns exec vnet$i ethtool --offload veth${i}-a rx off tx off

        # generate toml config
        cat <<EOL >> $toml_file
[node$i]
port_type="xdp"
if_name="veth${i}-b"
queue_id=0
mac_addr="$(printf 'aa:00:00:00:00:%02x' $i)"

EOL
    done
}

down() {
    num=$1

    for i in $(seq 0 $(($num - 1))); do
        sudo ip link delete veth${i}-b
        sudo ip netns delete vnet$i
    done
}

if [ -z "$2" ]; then
    echo "Usage: $0 <up|down> <num>"
    exit 1
fi

command=$1
num=$2

case $command in
    up)
        up $num
        ;;
    down)
        down $num
        ;;
    *)
        echo "Invalid command. Use 'up' or 'down'."
        exit 1
        ;;
esac

