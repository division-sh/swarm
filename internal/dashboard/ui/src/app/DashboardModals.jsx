import React from "react";
import MarkdownBlock from "../components/MarkdownBlock.jsx";
import Modal from "../components/Modal.jsx";
import HoldingVerticalDetail from "../features/holding/HoldingVerticalDetail.jsx";

export default function DashboardModals({
  modalContent,
  setModalContent,
  holdingDetailModal,
  setHoldingDetailModal,
}) {
  return (
    <>
      {modalContent ? (
        <Modal title={modalContent.title} onClose={() => setModalContent(null)} copyText={modalContent.text}>
          <MarkdownBlock text={modalContent.text} />
        </Modal>
      ) : null}
      {holdingDetailModal.open ? (
        <Modal
          title={`Holding Vertical — ${holdingDetailModal.data?.vertical?.slug || holdingDetailModal.data?.vertical?.name || holdingDetailModal.id || ""}`}
          onClose={() => setHoldingDetailModal({ open: false, loading: false, id: "", error: "", data: null })}
          copyText={holdingDetailModal.data ? JSON.stringify(holdingDetailModal.data, null, 2) : ""}
          className="holding-detail-modal"
        >
          {holdingDetailModal.loading ? (
            <div className="empty-state">Loading vertical detail...</div>
          ) : holdingDetailModal.error ? (
            <div className="health-bad">{holdingDetailModal.error}</div>
          ) : (
            <HoldingVerticalDetail detail={holdingDetailModal.data} />
          )}
        </Modal>
      ) : null}
    </>
  );
}
