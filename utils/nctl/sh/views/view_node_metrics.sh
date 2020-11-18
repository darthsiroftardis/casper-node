#!/usr/bin/env bash
#
# Renders node metrics to stdout.
# Globals:
#   NCTL - path to nctl home directory.
# Arguments:
#   Network ordinal identifier.
#   Node ordinal identifier.

#######################################
# Displays to stdout current node metrics.
# Globals:
#   NCTL - path to nctl home directory.
# Arguments:
#   Network ordinal identifier.
#   Node ordinal identifier.
#   Metric name.
#######################################
function _view_metrics() {
    endpoint=$(get_node_address_rest $1 $2)/metrics
    if [ $3 = "all" ]; then
        curl -s --location --request GET $endpoint  
    else
        echo "network #$1 :: node #$2 :: "$(curl -s --location --request GET $endpoint | grep $3 | tail -n 1)
    fi
}

#######################################
# Destructure input args.
#######################################

# Unset to avoid parameter collisions.
unset metric
unset net
unset node

for ARGUMENT in "$@"
do
    KEY=$(echo $ARGUMENT | cut -f1 -d=)
    VALUE=$(echo $ARGUMENT | cut -f2 -d=)
    case "$KEY" in
        metric) metric=${VALUE} ;;
        net) net=${VALUE} ;;
        node) node=${VALUE} ;;
        *)
    esac
done

# Set defaults.
metric=${metric:-"all"}
net=${net:-1}
node=${node:-"all"}

#######################################
# Main
#######################################

# Import utils.
source $NCTL/sh/utils/misc.sh

# Import vars.
source $(get_path_to_net_vars $net)

# Render metrics.
if [ $node = "all" ]; then
    for idx in $(seq 1 $NCTL_NET_NODE_COUNT)
    do
        _view_metrics $net $idx $metric
    done
else
    _view_metrics $net $node $metric
fi
