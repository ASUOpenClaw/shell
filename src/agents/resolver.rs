use std::collections::HashMap;

/// Maps workspace IDs to pre-configured OpenClaw agent IDs.
///
/// Agents are defined in the OpenClaw gateway's config (`agents.list[].id`).
/// This resolver does NOT spawn or terminate agents — it only performs a
/// config-based lookup: workspace_id → agent_id.
///
/// If no explicit mapping exists for a workspace, the default agent is used.
pub struct AgentResolver {
    map: HashMap<String, String>,
    default_agent: String,
}

impl AgentResolver {
    /// Build from a comma-separated mapping string ("ws1:agentA,ws2:agentB")
    /// and a fallback default agent ID.
    pub fn from_config(map_str: &str, default_agent: String) -> Self {
        let map = parse_agent_map(map_str);
        Self { map, default_agent }
    }

    /// Returns the agent_id for a workspace.
    /// Falls back to the default agent if no explicit mapping exists.
    pub fn resolve(&self, workspace_id: &str) -> &str {
        self.map
            .get(workspace_id)
            .map(String::as_str)
            .unwrap_or(&self.default_agent)
    }

    pub fn entries(&self) -> &HashMap<String, String> {
        &self.map
    }

    pub fn default_agent(&self) -> &str {
        &self.default_agent
    }
}

fn parse_agent_map(s: &str) -> HashMap<String, String> {
    s.split(',')
        .filter_map(|pair| {
            let mut parts = pair.trim().splitn(2, ':');
            let ws = parts.next()?.trim();
            let agent = parts.next()?.trim();
            if ws.is_empty() || agent.is_empty() {
                return None;
            }
            Some((ws.to_string(), agent.to_string()))
        })
        .collect()
}
