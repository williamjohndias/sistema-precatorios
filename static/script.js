// JavaScript para o Sistema de Precatórios

let modifiedData = {};
let originalData = {};
const SELECTED_STORAGE_KEY = 'selected_precatorios_ids';

// Inicialização
document.addEventListener('DOMContentLoaded', function() {
    initializeTable();
    setupSearch();
    setupEventListeners();
    syncSelectionFromStorage();
});

// Inicializar tabela
function initializeTable() {
    const table = document.getElementById('dataTable');
    const rows = table.querySelectorAll('tbody tr');
    
    // Armazenar dados originais
    rows.forEach(row => {
        const precatorioId = row.dataset.precatorio;
        originalData[precatorioId] = {};
        
        const cells = row.querySelectorAll('td[data-field]');
        cells.forEach(cell => {
            const field = cell.dataset.field;
            const fieldElement = cell.querySelector('.editable-field');
            const originalValue = fieldElement ? fieldElement.value : cell.dataset.original;
            originalData[precatorioId][field] = originalValue;
        });
    });
    
    updateModifiedCount();
}

// Configurar busca (removido - agora usamos filtros do servidor)
function setupSearch() {
    // Busca local removida - agora usamos filtros do servidor
}

// Configurar event listeners
function setupEventListeners() {
    // Adicionar listeners para campos editáveis
    const editableFields = document.querySelectorAll('.editable-field');
    editableFields.forEach(field => {
        field.addEventListener('blur', function() {
            validateField(this);
        });
        
        field.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                this.blur();
            }
        });
    });
    
    // Adicionar listeners para cabeçalhos ordenáveis
    const sortableHeaders = document.querySelectorAll('.sortable-header');
    sortableHeaders.forEach(header => {
        header.addEventListener('click', function() {
            sortByField(this.dataset.field);
        });
    });
}

// ===== Seleção persistente entre páginas =====
function getStoredSelectedIds() {
    try {
        const raw = localStorage.getItem(SELECTED_STORAGE_KEY);
        const arr = raw ? JSON.parse(raw) : [];
        return Array.isArray(arr) ? arr : [];
    } catch (e) {
        return [];
    }
}

function setStoredSelectedIds(ids) {
    const unique = Array.from(new Set(ids.map(String)));
    localStorage.setItem(SELECTED_STORAGE_KEY, JSON.stringify(unique));
}

function addIdsToSelection(ids) {
    const current = getStoredSelectedIds();
    setStoredSelectedIds(current.concat(ids.map(String)));
    reflectSelectionOnPage();
}

function removeIdsFromSelection(ids) {
    const removeSet = new Set(ids.map(String));
    const current = getStoredSelectedIds();
    const next = current.filter(id => !removeSet.has(String(id)));
    setStoredSelectedIds(next);
    reflectSelectionOnPage();
}

function clearStoredSelection() {
    localStorage.removeItem(SELECTED_STORAGE_KEY);
    reflectSelectionOnPage();
}

function syncSelectionFromStorage() {
    reflectSelectionOnPage();
}

function reflectSelectionOnPage() {
    const selectedIds = new Set(getStoredSelectedIds().map(String));
    const checkboxes = document.querySelectorAll('.row-checkbox');
    checkboxes.forEach(cb => {
        cb.checked = selectedIds.has(String(cb.value));
    });

    // Atualiza o estado do checkbox mestre
    const selectAllCheckbox = document.getElementById('selectAll');
    const allCount = checkboxes.length;
    const checkedCount = Array.from(checkboxes).filter(cb => cb.checked).length;
    if (checkedCount === 0) {
        selectAllCheckbox.checked = false;
        selectAllCheckbox.indeterminate = false;
    } else if (checkedCount === allCount) {
        selectAllCheckbox.checked = true;
        selectAllCheckbox.indeterminate = false;
    } else {
        selectAllCheckbox.checked = false;
        selectAllCheckbox.indeterminate = true;
    }

    // Atualiza visual do modal (se aberto)
    if (document.getElementById('bulkEditModal')) {
        updateSelectedRecordsDisplay();
    }
}

// Marcar campo como modificado
function markAsModified(field) {
    const cell = field.closest('td');
    const row = field.closest('tr');
    const precatorioId = row.dataset.precatorio;
    const fieldName = cell.dataset.field;
    
    if (!modifiedData[precatorioId]) {
        modifiedData[precatorioId] = {};
    }
    
    const currentValue = field.value;
    const originalValue = originalData[precatorioId][fieldName];
    
    // Só marcar como modificado se o valor realmente mudou
    if (originalValue !== currentValue) {
        modifiedData[precatorioId][fieldName] = currentValue;
        field.classList.add('modified');
    } else {
        // Se voltou ao valor original, remover da lista de modificações
        delete modifiedData[precatorioId][fieldName];
        field.classList.remove('modified');
        
        // Se não há mais modificações neste precatório, remover o objeto
        if (Object.keys(modifiedData[precatorioId]).length === 0) {
            delete modifiedData[precatorioId];
        }
    }
    
    updateModifiedCount();
}

// Validar campo
function validateField(field) {
    const fieldType = field.closest('td').dataset.type;
    const value = field.value.trim();
    
    // Remover classes de erro anteriores
    field.classList.remove('error');
    
    // Validações específicas por tipo
    if (fieldType === 'numeric') {
        if (value && isNaN(parseFloat(value))) {
            field.classList.add('error');
            showAlert('Valor numérico inválido', 'warning');
            return false;
        }
    }
    
    return true;
}

// Atualizar contador de modificações
function updateModifiedCount() {
    const count = Object.keys(modifiedData).length;
    const countElement = document.getElementById('modifiedCount');
    
    if (count > 0) {
        countElement.textContent = `${count} alteração(ões)`;
        countElement.className = 'badge bg-warning';
    } else {
        countElement.textContent = '0 alterações';
        countElement.className = 'badge bg-info';
    }
}

// Salvar alterações
function saveChanges() {
    if (Object.keys(modifiedData).length === 0) {
        showAlert('Nenhuma alteração para salvar', 'info');
        return;
    }
    
    // Mostrar resumo das alterações
    showChangesSummary();
    
    // Mostrar modal de confirmação
    const modal = new bootstrap.Modal(document.getElementById('confirmModal'));
    modal.show();
}

// Mostrar resumo das alterações
function showChangesSummary() {
    const summaryContainer = document.getElementById('changesSummary');
    let summaryHTML = '<h6>Alterações a serem salvas:</h6>';
    
    Object.keys(modifiedData).forEach(precatorioId => {
        summaryHTML += `<div class="change-item">
            <span class="field-name">Precatório ${precatorioId}:</span>
        </div>`;
        
        Object.keys(modifiedData[precatorioId]).forEach(fieldName => {
            const oldValue = originalData[precatorioId][fieldName] || '';
            const newValue = modifiedData[precatorioId][fieldName];
            
            summaryHTML += `<div class="change-item">
                <span class="field-name">${fieldName}:</span>
                <span class="field-change">
                    <span class="old-value">${oldValue}</span> → 
                    <span class="new-value">${newValue}</span>
                </span>
            </div>`;
        });
    });
    
    summaryContainer.innerHTML = summaryHTML;
}

// Confirmar salvamento
function confirmSave() {
    console.log('=== FUNÇÃO confirmSave CHAMADA ===');
    console.log('modifiedData:', modifiedData);
    console.log('Tipo de modifiedData:', typeof modifiedData);
    console.log('Chaves de modifiedData:', Object.keys(modifiedData));
    
    showLoading(true);
    
    // Verificar se há dados válidos
    if (Object.keys(modifiedData).length === 0) {
        console.log('NENHUM DADO MODIFICADO - RETORNANDO');
        showLoading(false);
        showAlert('Nenhuma alteração detectada', 'warning');
        return;
    }
    
    console.log('=== DEBUG FRONTEND ===');
    console.log('Dados modificados:', modifiedData);
    console.log('URL da requisição:', '/update');
    console.log('Iniciando fetch...');
    
    fetch('/update', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            data: modifiedData
        })
    })
    .then(response => {
        console.log('Resposta recebida:', response);
        console.log('Status:', response.status);
        console.log('OK:', response.ok);
        return response.json();
    })
    .then(data => {
        console.log('Dados da resposta:', data);
        showLoading(false);
        
        if (data.success) {
            showAlert(data.message, 'success');
            modifiedData = {};
            updateModifiedCount();
            
            // Fechar modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('confirmModal'));
            modal.hide();
            
            // Recarregar dados após 2 segundos
            setTimeout(() => {
                refreshData();
            }, 2000);
        } else {
            showAlert(data.message, 'danger');
        }
    })
    .catch(error => {
        console.log('ERRO na requisição:', error);
        showLoading(false);
        showAlert('Erro ao salvar alterações: ' + error.message, 'danger');
    });
}

// Desfazer alterações
function undoChanges() {
    if (Object.keys(modifiedData).length === 0) {
        showAlert('Nenhuma alteração para desfazer', 'info');
        return;
    }
    
    // Restaurar valores originais
    Object.keys(modifiedData).forEach(precatorioId => {
        Object.keys(modifiedData[precatorioId]).forEach(fieldName => {
            const cell = document.querySelector(`tr[data-precatorio="${precatorioId}"] td[data-field="${fieldName}"]`);
            const field = cell.querySelector('.editable-field');
            
            if (field) {
                field.value = originalData[precatorioId][fieldName] || '';
                field.classList.remove('modified', 'error');
            }
        });
    });
    
    // Limpar dados modificados
    modifiedData = {};
    updateModifiedCount();
    
    showAlert('Alterações desfeitas com sucesso', 'success');
}

// Atualizar dados
// Mostrar alerta
function showAlert(message, type) {
    const alertContainer = document.getElementById('alertContainer');
    const alertId = 'alert-' + Date.now();
    
    const alertHTML = `
        <div id="${alertId}" class="alert alert-${type} alert-dismissible fade show" role="alert">
            <i class="fas fa-${getAlertIcon(type)} me-2"></i>
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;
    
    alertContainer.insertAdjacentHTML('beforeend', alertHTML);
    
    // Auto-remover após 5 segundos
    setTimeout(() => {
        const alert = document.getElementById(alertId);
        if (alert) {
            alert.remove();
        }
    }, 5000);
}

// Obter ícone do alerta
function getAlertIcon(type) {
    const icons = {
        'success': 'check-circle',
        'danger': 'exclamation-triangle',
        'warning': 'exclamation-circle',
        'info': 'info-circle'
    };
    return icons[type] || 'info-circle';
}

// Mostrar/esconder loading
function showLoading(show) {
    const overlay = document.getElementById('loadingOverlay');
    if (show) {
        overlay.classList.add('show');
    } else {
        overlay.classList.remove('show');
    }
}

// Atalhos de teclado
document.addEventListener('keydown', function(e) {
    // Ctrl+S para salvar
    if (e.ctrlKey && e.key === 's') {
        e.preventDefault();
        saveChanges();
    }
    
    // Ctrl+Z para desfazer
    if (e.ctrlKey && e.key === 'z') {
        e.preventDefault();
        undoChanges();
    }
    
    // F5 para atualizar
    if (e.key === 'F5') {
        e.preventDefault();
        refreshData();
    }
});

// Função para ordenar por campo
function sortByField(field) {
    const urlParams = new URLSearchParams(window.location.search);
    const currentSort = urlParams.get('sort') || 'ordem';
    const currentOrder = urlParams.get('order') || 'asc';
    
    // Determinar nova ordem
    let newOrder = 'asc';
    if (currentSort === field && currentOrder === 'asc') {
        newOrder = 'desc';
    }
    
    // Atualizar parâmetros da URL
    urlParams.set('sort', field);
    urlParams.set('order', newOrder);
    urlParams.set('page', '1'); // Voltar para primeira página
    
    // Redirecionar com novos parâmetros
    window.location.href = window.location.pathname + '?' + urlParams.toString();
}

// ===== FUNCIONALIDADES DE EDIÇÃO EM MASSA =====

// Abrir modal de edição em massa
function openBulkEditModal() {
    const modal = new bootstrap.Modal(document.getElementById('bulkEditModal'));
    updateSelectedRecordsDisplay();
    populateEditableFields();
    modal.show();
}

// Atualizar display dos registros selecionados
function updateSelectedRecordsDisplay() {
    const selectedIds = getStoredSelectedIds();
    const selectedRecordsDiv = document.getElementById('selectedRecords');
    const selectionInfoSpan = document.getElementById('selectionInfo');

    if (!selectedIds || selectedIds.length === 0) {
        selectedRecordsDiv.innerHTML = '<small class="text-muted">Nenhum registro selecionado</small>';
        if (selectionInfoSpan) {
            selectionInfoSpan.textContent = 'Selecione registros para edição em massa';
        }
        return;
    }

    let html = `<div class="mb-2"><strong>${selectedIds.length} registro(s) selecionado(s):</strong></div>`;

    // Mostra detalhado apenas para os visíveis na página atual
    const visibleById = {};
    document.querySelectorAll('tbody tr').forEach(row => {
        const id = row.getAttribute('data-precatorio');
        if (!id) return;
        const precatorioCell = row.querySelector('[data-field="precatorio"]');
        const organizacaoCell = row.querySelector('[data-field="organizacao"]');
        visibleById[id] = {
            precatorio: precatorioCell ? precatorioCell.textContent.trim() : id,
            organizacao: organizacaoCell ? organizacaoCell.textContent.trim() : 'N/A'
        };
    });

    selectedIds.forEach(id => {
        if (visibleById[id]) {
            html += `<div class="d-flex justify-content-between align-items-center mb-1">
                        <small>${visibleById[id].precatorio}</small>
                        <small class="text-muted">${visibleById[id].organizacao}</small>
                     </div>`;
        }
    });

    const hiddenCount = selectedIds.filter(id => !visibleById[id]).length;
    if (hiddenCount > 0) {
        html += `<div class="mt-2"><small class="text-muted">+ ${hiddenCount} selecionado(s) em outras páginas</small></div>`;
    }

    selectedRecordsDiv.innerHTML = html;
    
    // Atualizar informação de seleção
    if (selectionInfoSpan) {
        if (hiddenCount > 0) {
            selectionInfoSpan.textContent = `${selectedIds.length} registros selecionados (${selectedIds.length - hiddenCount} nesta página, ${hiddenCount} em outras páginas)`;
        } else {
            selectionInfoSpan.textContent = `${selectedIds.length} registros selecionados nesta página`;
        }
    }
}

// Popular campos editáveis no modal
function populateEditableFields() {
    const editableFieldsDiv = document.getElementById('editableFields');
    const editableFields = [
        { name: 'situacao', label: 'Situação', type: 'text' },
        { name: 'valor', label: 'Valor', type: 'text' }
    ];
    
    let html = '';
    editableFields.forEach(field => {
        html += `
            <div class="mb-3">
                <label class="form-label">${field.label}:</label>
                <input type="${field.type}" 
                       class="form-control form-control-sm" 
                       id="bulk_${field.name}" 
                       placeholder="Novo valor para ${field.label}">
                <small class="form-text text-muted">Deixe vazio para não alterar este campo</small>
            </div>
        `;
    });
    
    editableFieldsDiv.innerHTML = html;
}

// Selecionar todos os registros visíveis
function selectAllVisible() {
    const checkboxes = document.querySelectorAll('.row-checkbox');
    const ids = Array.from(checkboxes).map(cb => cb.value);
    addIdsToSelection(ids);
}

// Selecionar todas as páginas
function selectAllPages() {
    showLoading(true);
    
    // Buscar todos os IDs do banco
    fetch('/api/get_all_ids')
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            addIdsToSelection(data.ids);
            showAlert(`Selecionados ${data.ids.length} registros de todas as páginas`, 'success');
        } else {
            showAlert('Erro ao carregar IDs: ' + data.message, 'danger');
        }
    })
    .catch(error => {
        showAlert('Erro ao selecionar todas as páginas: ' + error.message, 'danger');
    })
    .finally(() => {
        showLoading(false);
    });
}

// Limpar seleção
function clearSelection() {
    clearStoredSelection();
}

// Toggle selecionar todos
function toggleSelectAll() {
    const selectAllCheckbox = document.getElementById('selectAll');
    const checkboxes = document.querySelectorAll('.row-checkbox');
    const ids = Array.from(checkboxes).map(cb => cb.value);
    if (selectAllCheckbox.checked) {
        addIdsToSelection(ids);
    } else {
        removeIdsFromSelection(ids);
    }
}

// Confirmar atualização em massa
function confirmBulkUpdate() {
    const selectedIds = getStoredSelectedIds();

    if (selectedIds.length === 0) {
        showAlert('Selecione pelo menos um registro para atualizar', 'warning');
        return;
    }
    
    // Coletar campos para atualizar
    const fieldUpdates = {};
    const editableFields = ['situacao', 'valor'];
    
    editableFields.forEach(field => {
        const input = document.getElementById(`bulk_${field}`);
        if (input && input.value.trim() !== '') {
            fieldUpdates[field] = input.value.trim();
        }
    });
    
    if (Object.keys(fieldUpdates).length === 0) {
        showAlert('Preencha pelo menos um campo para atualizar', 'warning');
        return;
    }
    
    // Mostrar loading
    showLoading(true);
    
    // Fazer requisição
    fetch('/bulk_update', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            selected_ids: selectedIds,
            field_updates: fieldUpdates
        })
    })
    .then(response => response.json())
    .then(data => {
        showLoading(false);
        
        if (data.success) {
            showAlert(data.message, 'success');
            
            // Fechar modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('bulkEditModal'));
            modal.hide();
            
            // Limpar seleção
            clearStoredSelection();
            
            // Recarregar dados após 2 segundos
            setTimeout(() => {
                refreshData();
            }, 2000);
        } else {
            showAlert(data.message, 'danger');
        }
    })
    .catch(error => {
        showLoading(false);
        showAlert('Erro ao atualizar registros: ' + error.message, 'danger');
    });
}

// Configurar event listeners para checkboxes
document.addEventListener('change', function(e) {
    if (e.target.classList.contains('row-checkbox')) {
        const id = e.target.value;
        if (e.target.checked) {
            addIdsToSelection([id]);
        } else {
            removeIdsFromSelection([id]);
        }
    }
});

// Confirmar antes de sair se há alterações não salvas
window.addEventListener('beforeunload', function(e) {
    if (Object.keys(modifiedData).length > 0) {
        e.preventDefault();
        e.returnValue = 'Há alterações não salvas. Deseja realmente sair?';
    }
});
