#!/usr/bin/env bash
#
# Renders a user secret key path.
# Globals:
#   NCTL - path to nctl home directory.
# Arguments:
#   Network ordinal identifier.
#   User ordinal identifier.

#######################################
# Destructure input args.
#######################################

# Unset to avoid parameter collisions.
unset net
unset node

for ARGUMENT in "$@"
do
    KEY=$(echo $ARGUMENT | cut -f1 -d=)
    VALUE=$(echo $ARGUMENT | cut -f2 -d=)
    case "$KEY" in
        net) net=${VALUE} ;;
        node) node=${VALUE} ;;
        *)
    esac
done

# Set defaults.
net=${net:-1}
node=${node:-"all"}

#######################################
# Main
#######################################

# Import utils.
source $NCTL/sh/utils/misc.sh

# Import vars.
source $(get_path_to_net_vars $net)

# Render path to secret key(s).
if [ $node = "all" ]; then
    for idx in $(seq 1 $NCTL_NET_NODE_COUNT)
    do
        render_account_secret_key $net $NCTL_ACCOUNT_TYPE_NODE $idx
    done
else
    render_account_secret_key $net $NCTL_ACCOUNT_TYPE_NODE $node
fi
