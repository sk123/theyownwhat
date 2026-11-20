import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'
import { ReviewerModeProvider, ReviewerModeBanner } from './context/ReviewerModeContext.jsx'
import ReviewerReportModal from './components/ReviewerReportModal.jsx'
import { initInterestTooltips } from './utils/interestTooltips.js'

initInterestTooltips()

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <ReviewerModeProvider>
      <ReviewerModeBanner />
      <App />
      <ReviewerReportModal />
    </ReviewerModeProvider>
  </StrictMode>,
)
