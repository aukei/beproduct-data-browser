"""
Shared "Delete Record" confirmation dialog with referential impact warnings.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional

import streamlit as st

logger = logging.getLogger(__name__)


def show_delete_confirmation_dialog(
    entity_type: str,
    record_id: str,
    display_name: str,
    on_delete_callback: Callable[[str], tuple[bool, str]],
    referential_impacts: Optional[list[dict[str, Any]]] = None,
) -> None:
    """
    Show a delete confirmation dialog with referential impact warnings.
    
    Args:
        entity_type: One of "Style", "Material", "Color", "Image", "Block", "Directory", "User"
        record_id: ID of the record to delete
        display_name: Human-readable name (e.g., style number + name)
        on_delete_callback: Function(record_id) -> tuple[bool, str] that performs deletion
        referential_impacts: Optional list of dicts describing dependent records
                            Each dict should have: {entity_type, entity_id, header_number, ...}
    """
    entity_type_lower = entity_type.lower()
    
    with st.dialog(f"🗑️ Delete {entity_type}"):
        st.error(f"⚠️ Delete {entity_type.lower()}: {display_name}")
        
        # Show referential impact warnings
        if referential_impacts:
            st.warning(f"**This {entity_type.lower()} is referenced by:**")
            
            # Group by entity type
            impacts_by_type = {}
            for impact in referential_impacts:
                impact_type = impact.get("entity_type", "Unknown")
                if impact_type not in impacts_by_type:
                    impacts_by_type[impact_type] = []
                impacts_by_type[impact_type].append(impact)
            
            # Display grouped impacts
            for impact_type, impacts_list in impacts_by_type.items():
                st.write(f"- **{len(impacts_list)} {impact_type}(s)**:")
                
                # Show sample references
                for impact in impacts_list[:3]:  # Show first 3 samples
                    ref_label = impact.get("header_number", impact.get("name", impact.get("entity_id", "Unknown")))
                    
                    # Add count info if available
                    if "colorway_count" in impact:
                        st.write(f"  - {ref_label} ({impact['colorway_count']} colorway refs)")
                    elif "field_name" in impact:
                        st.write(f"  - {ref_label} (field: {impact['field_name']})")
                    else:
                        st.write(f"  - {ref_label}")
                
                # Show if more exist
                if len(impacts_list) > 3:
                    st.write(f"  ... and {len(impacts_list) - 3} more")
            
            st.info("Deleting this record may break references in the listed records.")
        
        # Confirmation checkbox
        confirmed = st.checkbox(
            f"I understand. Permanently delete this {entity_type.lower()}.",
            key=f"delete_confirm_{record_id}",
        )
        
        # Delete button
        col1, col2 = st.columns(2)
        with col1:
            delete_clicked = st.button(
                f"🗑️ Delete {entity_type}",
                use_container_width=True,
                disabled=not confirmed,
                type="secondary",
                key=f"delete_btn_{record_id}",
            )
        with col2:
            cancel_clicked = st.button(
                "Cancel",
                use_container_width=True,
                key=f"delete_cancel_{record_id}",
            )
        
        if delete_clicked:
            with st.spinner(f"Deleting {entity_type.lower()}…"):
                success, message = on_delete_callback(record_id)
                
                if success:
                    st.success(message)
                    st.session_state[f"deleted_{entity_type_lower}_id"] = record_id
                    st.rerun()
                else:
                    st.error(message)
        
        if cancel_clicked:
            st.rerun()
